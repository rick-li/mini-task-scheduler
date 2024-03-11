import datetime
import sys
import time
from db import get_conn
from worker import start_workers

leader_timeout = 5

def is_leader_alive(conn):
    current_time = datetime.datetime.utcnow()
    with conn.cursor() as cur:
        cur.execute("SELECT node_id, last_heartbeat FROM leader_election WHERE id = 1;")
        leader_info = cur.fetchone()
        if leader_info and (current_time - leader_info[1]).total_seconds() < leader_timeout:
            return True,leader_info[0]
        return False, None

def claim_leadership(conn, node_id):
    current_time = datetime.datetime.utcnow()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.execute("SELECT node_id FROM leader_election WHERE id = 1 FOR UPDATE;")
            cur.execute("UPDATE leader_election SET node_id = %s, last_heartbeat = %s WHERE id = 1;", (node_id, current_time))
            cur.execute("COMMIT;")
        return True
    except Exception as e:
        print("An error occurred: ", e)
        conn.rollback()
        return False
    
def send_heartbeat(conn):
    current_time = datetime.datetime.utcnow()
    with conn.cursor() as cur:
        cur.execute("UPDATE leader_election SET last_heartbeat = %s WHERE node_id = %s;", (current_time, node_id))
        conn.commit()

def start_leader_election():
    conn = get_conn()
    node_id = 'node1'
    if len(sys.argv) > 1:
        node_id = sys.argv[1]
    else:
        print("Please provide node_id as a command line argument.")
        sys.exit(1)
    while True:
        leader_alive, current_leader = is_leader_alive(conn)
        if not leader_alive:
            print("Leader is dead, claiming leadership")
            if claim_leadership(conn, node_id):
                print("Leadership claimed")
            else:
                print("Failed to claim leadership")
        elif leader_alive and current_leader == node_id:
            send_heartbeat(conn)
            print(f"{node_id} is the leader and sent a heartbeat")
        else:
            print(f"{node_id} is the waiting, current leader is {current_leader}")
        time.sleep(1)

if __name__ == "__main__":
   start_workers(2)
   