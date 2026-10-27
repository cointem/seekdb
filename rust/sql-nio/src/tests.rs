// Copyright (c) 2025 OceanBase.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Commit state machine tests: real nonblocking transport, no database.
use crate::*;

fn connection() -> (Arc<Conn>, std::net::TcpStream) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (socket, _) = listener.accept().unwrap();
    socket.set_nonblocking(true).unwrap();
    let stream = ConnStream::from(TcpStream::from_std(socket));
    let poll = Poll::new().unwrap();
    let token = Token(FIRST_CONN);
    let conn = Arc::new(Conn {
        token,
        server_caps: capability::server_capabilities(false),
        tls_config: None,
        mu: Mutex::new(ConnInner {
            sock: stream,
            tls: None,
            expected_login_seq: 1,
            tls_arm_pending: false,
            inbuf: Vec::new(),
            rpos: 0,
            drain_stopped_at_budget: false,
            outbuf: Vec::new(),
            out_pos: 0,
            want_write: false,
            write_waiters: 0,
            profile_commit_ns: 0,
            final_response: FinalResponseState::Idle,
            response: ResponseWriter::new(),
            read_state: ReadState::Login,
            inner_buf: Vec::new(),
            inner_rpos: 0,
            compressed_wire_bytes: 0,
            mid_request_body: None,
            next_mid_request_lease: 0,
            active_request_body: None,
            initial_body_spare: Vec::new(),
            req_compressed: false,
            next_comp_seq: 0,
            login: None,
        }),
        write_done: Condvar::new(),
        mid_read_signal: Mutex::new(MidReadSignal::default()),
        mid_read_ready: Condvar::new(),
        session_storage: CppSessionStorage::new(16),
        reg: Arc::new(poll.registry().try_clone().unwrap()),
        waker: Arc::new(Waker::new(poll.registry(), WAKER).unwrap()),
        reactor_polling: Arc::new(AtomicBool::new(false)),
        closes: Arc::new(Mutex::new(Vec::new())),
        ready: Arc::new(Mutex::new(Vec::new())),
        commits: Arc::new(Mutex::new(Vec::new())),
        completions: Arc::new(Mutex::new(Vec::new())),
        err: AtomicBool::new(false),
        peer_closed: AtomicBool::new(false),
        need_shutdown: AtomicBool::new(false),
        sql_session_bound: AtomicBool::new(false),
        max_packet_size: AtomicUsize::new(MAX_LOGIN_BODY_LEN),
        raw_caps: AtomicU32::new(0),
        transport_caps: AtomicU32::new(0),
        session_constructed: AtomicBool::new(false),
        disconnect_notified: AtomicBool::new(false),
        close_notified: AtomicBool::new(false),
        close_requested: AtomicBool::new(false),
        request_gate: Mutex::new(()),
        request_state: AtomicU8::new(0),
        request_generation: AtomicU64::new(0),
    });
    (conn, peer)
}

fn stage(conn: &Arc<Conn>, bytes: &[u8]) {
    conn.request_state.store(REQUEST_BUSY, Ordering::Release);
    conn.request_generation.store(7, Ordering::Release);
    let mut g = conn.mu.lock().unwrap();
    assert!(g.response.begin(7, 1));
    g.outbuf.extend_from_slice(bytes);
    g.final_response = FinalResponseState::Prepared(RequestContinuation {
        generation: 7,
        auth_outcome: AuthOutcome::NoChange,
    });
}
fn commit(conn: &Arc<Conn>, generation: u64) {
    let mut handle = NioConnectionHandle { conn: conn.clone() };
    unsafe {
        nio_commit_request(&mut handle, generation);
    }
}
#[test]
fn small_response_finishes_without_reactor_and_stale_commit_is_ignored() {
    let (conn, mut peer) = connection();
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    stage(&conn, b"packet");
    commit(&conn, 6);
    assert!(conn.request_is_busy());
    assert!(conn.commits.lock().unwrap().is_empty());
    commit(&conn, 7);
    let mut bytes = [0; 6];
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"packet");
    assert!(!conn.request_is_busy());
    assert_eq!(
        conn.mu.lock().unwrap().final_response,
        FinalResponseState::Idle
    );
    assert!(conn.commits.lock().unwrap().is_empty());
    commit(&conn, 7);
    assert!(conn.commits.lock().unwrap().is_empty());
}
#[test]
fn finish_preserves_buffered_input_and_readable_edge() {
    for buffered in [false, true] {
        let (conn, _peer) = connection();
        stage(&conn, b"ok");
        if buffered {
            conn.mu.lock().unwrap().inbuf.push(1);
        } else {
            conn.request_state
                .fetch_or(REQUEST_READ_READY, Ordering::AcqRel);
        }
        commit(&conn, 7);
        assert!(!conn.request_is_busy());
        assert_eq!(*conn.ready.lock().unwrap(), vec![conn.token]);
    }
}
#[test]
fn tls_compressed_armed_and_large_responses_preserve_reactor_ownership() {
    for mode in 0..4 {
        let (conn, _peer) = connection();
        let bytes = vec![
            9;
            if mode == 2 {
                RESPONSE_BATCH_TARGET + 1
            } else {
                5
            }
        ];
        stage(&conn, &bytes);
        {
            let mut g = conn.mu.lock().unwrap();
            g.req_compressed = mode == 0;
            g.want_write = mode == 1;
            if mode == 3 {
                #[derive(Debug)]
                struct NoCertificate;
                impl rustls::server::ResolvesServerCert for NoCertificate {
                    fn resolve(
                        &self,
                        _: rustls::server::ClientHello<'_>,
                    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
                        None
                    }
                }
                let config = Arc::new(
                    rustls::ServerConfig::builder()
                        .with_no_client_auth()
                        .with_cert_resolver(Arc::new(NoCertificate)),
                );
                g.tls = Some(TlsSession::new(&config).unwrap());
            }
        }
        commit(&conn, 7);
        assert!(conn.request_is_busy());
        assert_eq!(conn.commits.lock().unwrap().len(), 1);
        let g = conn.mu.lock().unwrap();
        assert_eq!(g.out_pos, 0);
        assert_eq!(g.outbuf, bytes);
    }
}
#[test]
fn socket_backpressure_keeps_exact_unsent_suffix() {
    let (conn, mut peer) = connection();
    let filler = [3u8; 65536];
    let mut sent = 0;
    {
        let mut g = conn.mu.lock().unwrap();
        loop {
            match g.sock.write(&filler) {
                Ok(0) => break,
                Ok(n) => sent += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("{e}"),
            }
        }
    }
    stage(&conn, b"tail");
    commit(&conn, 7);
    assert!(conn.request_is_busy());
    assert_eq!(conn.commits.lock().unwrap().len(), 1);
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let mut received = vec![0; sent];
    peer.read_exact(&mut received).unwrap();
    assert!(received.iter().all(|v| *v == 3));
    {
        let mut g = conn.mu.lock().unwrap();
        assert!(flush_buffer_locked(&conn, &mut g));
        let generation = take_final_completion_locked(&mut g).unwrap();
        let finished = finish_worker_request_locked(&conn, &mut g, generation).unwrap();
        drop(g);
        publish_finish(&conn, finished);
    }
    let mut tail = [0; 4];
    peer.read_exact(&mut tail).unwrap();
    assert_eq!(&tail, b"tail");
    assert!(!conn.request_is_busy());
}
#[test]
fn fatal_write_uses_existing_close_and_completion_path() {
    let (conn, _peer) = connection();
    stage(&conn, b"packet");
    conn.mu
        .lock()
        .unwrap()
        .sock
        .shutdown(std::net::Shutdown::Write)
        .unwrap();
    commit(&conn, 7);
    assert!(conn.err.load(Ordering::Acquire));
    assert!(conn.close_requested.load(Ordering::Acquire));
    assert!(!conn.request_is_busy());
    assert!(conn.commits.lock().unwrap().is_empty());
}
#[test]
fn single_attempt_never_retries_or_loses_partial_progress() {
    struct Writer {
        result: Option<std::io::Result<usize>>,
        calls: usize,
    }
    impl Write for Writer {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            self.result.take().unwrap()
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    for kind in [
        std::io::ErrorKind::WouldBlock,
        std::io::ErrorKind::Interrupted,
    ] {
        let mut w = Writer {
            result: Some(Err(std::io::Error::from(kind))),
            calls: 0,
        };
        assert_eq!(request::write_once(&mut w, b"abcd").unwrap(), 0);
        assert_eq!(w.calls, 1);
    }
    let mut w = Writer {
        result: Some(Ok(2)),
        calls: 0,
    };
    assert_eq!(request::write_once(&mut w, b"abcd").unwrap(), 2);
    assert_eq!(w.calls, 1);
    let mut w = Writer {
        result: Some(Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))),
        calls: 0,
    };
    assert!(request::write_once(&mut w, b"abcd").is_err());
    assert_eq!(w.calls, 1);
}

// Production callbacks are provided by C++; this standalone harness never dispatches SQL.
#[no_mangle]
extern "C" fn ob_sql_sock_handler_on_connect(
    _: *mut c_void,
    _: *mut c_void,
    _: c_int,
    _: c_int,
    _: *mut NioGreetingInfo,
) -> c_int {
    -1
}
#[no_mangle]
extern "C" fn ob_sql_sock_handler_on_readable(
    _: *mut c_void,
    _: *mut c_void,
    _: *mut c_char,
    _: i64,
    _: u64,
    _: c_int,
    _: *const NioMysqlCommandView,
    _: u64,
) -> c_int {
    -1
}
#[no_mangle]
extern "C" fn ob_sql_sock_handler_on_disconnect(_: *mut c_void, _: *mut c_void) {}
#[no_mangle]
extern "C" fn ob_sql_sock_handler_on_close(_: *mut c_void, _: *mut c_void, _: c_int) {}
