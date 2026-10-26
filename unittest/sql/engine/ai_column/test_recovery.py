#!/usr/bin/env python3
"""AI-column contract and recovery checks against a dedicated seekdb instance.

Run --phase prepare, restart that instance, then run --phase verify.
The caller supplies the official mock endpoint (ai_column_mock.py).
"""
import argparse
import json
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', required=True)
    parser.add_argument('--port', type=int, default=12883)
    parser.add_argument('--mock-port', type=int, default=19120)
    parser.add_argument('--phase', choices=['prepare', 'verify'], required=True)
    args = parser.parse_args()
    command = [args.client, '--no-defaults', '-h127.0.0.1', '-P' + str(args.port),
               '-uroot', '-N', '-B', '--raw', 'test']

    def sql(statement, error=False):
        result = subprocess.run(command, input=statement, text=True, capture_output=True, timeout=30)
        assert (result.returncode != 0) == error, result.stderr + '\n' + statement
        return result.stdout.strip()

    def wait(statement, expected):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if sql(statement) == expected:
                return
            time.sleep(.1)
        raise AssertionError(statement + '\n' + sql(statement))

    key = "table_id=(SELECT table_id FROM oceanbase.__all_table WHERE table_name='ai_recovery_contract')"
    if args.phase == 'prepare':
        sql("CREATE TABLE ai_recovery_contract(id BIGINT PRIMARY KEY, content VARCHAR(200));")
        sql("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL('recovery_contract_model',"
            "'{\"type\":\"completion\",\"model_name\":\"mock-complete\"}');")
        config = json.dumps({'ai_model_name': 'recovery_contract_model',
                             'url': f'http://127.0.0.1:{args.mock_port}/v1/chat/completions',
                             'access_key': 'test', 'provider': 'openai', 'request_model_name': 'mock-complete'})
        sql("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('recovery_contract_ep','" + config + "');")
        sql("ALTER TABLE ai_recovery_contract ADD COLUMN result JSON AI COLUMN "
            "(AI_COMPLETE('recovery_contract_model',content));")
        assert "AI COLUMN (AI_COMPLETE('recovery_contract_model',content))" in sql('SHOW CREATE TABLE ai_recovery_contract;')
        assert 'content' in sql('SHOW AI COLUMNS FROM ai_recovery_contract;')
        sql("INSERT INTO ai_recovery_contract(id,content,result) VALUES(99,'blocked','{}');", error=True)
        sql("INSERT INTO ai_recovery_contract(id,content,result) SELECT 99,'blocked','{}';", error=True)
        sql("UPDATE ai_recovery_contract SET result='{}';", error=True)
        sql("INSERT INTO ai_recovery_contract(id,content) VALUES(1,'before-restart');")
        wait("SELECT JSON_UNQUOTE(JSON_EXTRACT(result,'$.input')) FROM ai_recovery_contract WHERE id=1;", 'before-restart')
        # Commit a claimed task whose owner disappears at restart. Future lease
        # expiry ensures it cannot finish before the caller restarts the server.
        sql("START TRANSACTION; UPDATE ai_recovery_contract SET content='after-restart' WHERE id=1;"
            "UPDATE oceanbase.__all_ai_column_task SET state=2,lease_owner='abandoned-owner',"
            "lease_expiry=TIMESTAMPADD(HOUR,1,NOW(6)) WHERE " + key + "; COMMIT;")
        assert sql("SELECT COUNT(*) FROM oceanbase.__all_ai_column_task WHERE " + key + " AND state=2;") == '1'
        print('PASS: ALTER ADD, SHOW CREATE, result write protection; claimed task committed for restart')
    else:
        assert sql("SELECT COUNT(*) FROM oceanbase.__all_ai_column_task WHERE " + key + " AND state=2;") == '1'
        assert sql('SELECT result IS NULL FROM ai_recovery_contract WHERE id=1;') == '1'
        sql("UPDATE oceanbase.__all_ai_column_task SET lease_expiry=TIMESTAMPADD(SECOND,-1,NOW(6)) WHERE " + key + ";")
        wait("SELECT JSON_UNQUOTE(JSON_EXTRACT(result,'$.input')) FROM ai_recovery_contract WHERE id=1;", 'after-restart')
        assert sql("SELECT COUNT(*) FROM oceanbase.__all_ai_column_task WHERE " + key + ";") == '0'
        # One bad result must neither strand its task nor hide successful siblings.
        sql("CREATE TABLE ai_conversion_contract(id BIGINT PRIMARY KEY, content VARCHAR(100),"
            "short_result VARCHAR(2) AI COLUMN(AI_COMPLETE('recovery_contract_model',content)) WITH(max_retries=0),"
            "valid_result JSON AI COLUMN(AI_COMPLETE('recovery_contract_model',content)));"
            "INSERT INTO ai_conversion_contract(id,content) VALUES(1,'long-output');")
        wait('SELECT valid_result IS NOT NULL AND short_result IS NULL FROM ai_conversion_contract;', '1')
        sql('SET ai_column_strict_read=ON; SELECT short_result FROM ai_conversion_contract;', error=True)
        assert sql('SET ai_column_strict_read=ON; SELECT valid_result IS NOT NULL FROM ai_conversion_contract;') == '1'
        assert sql("SELECT COUNT(*) FROM oceanbase.__all_ai_column_task WHERE table_id="
                   "(SELECT table_id FROM oceanbase.__all_table WHERE table_name='ai_conversion_contract');") == '0'
        sql('DROP TABLE ai_conversion_contract,ai_recovery_contract;'
            "CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('recovery_contract_ep');"
            "CALL DBMS_AI_SERVICE.DROP_AI_MODEL('recovery_contract_model');")
        print('PASS: durable task recovery, expired lease reclaim, conversion failure and atomic sibling publication')


if __name__ == '__main__':
    main()
