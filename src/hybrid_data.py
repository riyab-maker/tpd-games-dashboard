import pymysql
import csv

HOST = 'a9c3c220991da47c895500da385432b6-1807075149.ap-south-1.elb.amazonaws.com'
PORT = 3310
DBNAME = 'live'
USER = 'techadmin'
PASSWORD = 'Rl@ece@1234'


def main():
    try:
        with pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            connect_timeout=10,
            ssl={'ssl': {}},
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                print("Connected. Test query:", cur.fetchone())

            with conn.cursor() as cur:
                print("\nRunning custom query:")
                sql = (
                    """
                    SELECT 
                      `matomo_log_link_visit_action`.`idlink_va`,
                      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
                      `matomo_log_link_visit_action`.`idvisit`,
                      DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) AS server_time,
                      `matomo_log_link_visit_action`.`idaction_name`,
                      `matomo_log_link_visit_action`.`custom_dimension_2`,
                      CASE 
                        WHEN `matomo_log_link_visit_action`.`idaction_name` IN (
                          7228,16088,23560,34234,47426,47479,47066,46997,47994,48428,
                          47910,49078,48834,48883,48573,49214,49663,49719,49995,49976,
                          50099,49525,49395,51134,50812,51603,51627
                        ) THEN 'Started'
                        ELSE 'Completed'
                      END AS event
                    FROM `matomo_log_link_visit_action`
                    WHERE `matomo_log_link_visit_action`.`idaction_name` IN (
                        7228,16088,16204,23560,23592,34234,34299,
                        47426,47472,47479,47524,47066,47099,46997,47001,
                        47994,47998,48428,48440,47910,47908,49078,49113,
                        48834,48835,48883,48919,48573,48607,49214,49256,
                        49663,49698,49719,49721,49995,50051,49976,49978,
                        50099,50125,49525,49583,49395,49470,51134,51209,
                        50812,50846,51603,51607,51627,51635
                    )
                    AND `matomo_log_link_visit_action`.`custom_dimension_2` IN (
                        "12","28","24","40","54","56","50","52","70","72",
                        "58","66","68","60","62","64","78","80","82","84",
                        "83","76","74","88","86","94","96"
                    )
                    AND `matomo_log_link_visit_action`.`server_time` >= '2025-07-02'
                    """
                )
                cur.execute(sql)
                rows = cur.fetchall()
                column_names = [desc[0] for desc in cur.description]
                print("Columns:", ", ".join(column_names))
                print("-" * 80)
                if rows:
                    for i, row in enumerate(rows, 1):
                        print(f"Row {i}: {row}")
                else:
                    print("No rows matched the query.")
    except Exception as e:
        print("Connection/metadata error:", e)


if __name__ == "__main__":
    main()