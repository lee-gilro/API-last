from email.mime import application
from sqlite3 import Row
from urllib import response
import pymysql
from flask import jsonify
from flask import Flask
from flask import flash, request
from flaskext.mysql import MySQL
from flask_cors import CORS, cross_origin
import solana
import asyncio
from solana.rpc.async_api import AsyncClient
from solana.rpc.api import Client
from solana.keypair import Keypair
from solana.transaction import Transaction
from solana.system_program import TransferParams, transfer
from base58 import b58encode
from bitstring import BitArray, ConstBitStream
import time


application = Flask(__name__)
CORS(application)
mysql = MySQL()
application.config['MYSQL_DATABASE_USER'] = 'memorics'
application.config['MYSQL_DATABASE_PASSWORD'] = 'qwer12#$'
application.config['MYSQL_DATABASE_DB'] = 'msx'
application.config['MYSQL_DATABASE_HOST'] = 'p2pservice-rds-prod.cf219cl1yo7q.ap-northeast-2.rds.amazonaws.com'
application.config['MYSQL_DATABASE_PORT'] = 3306
mysql.init_app(application)



@application.route('/referralAllowance', methods=['POST'])
def ref_allowance():
    try:        
        _json = request.json
        _from_id = _json['from_id']
        _to_id = _json['to_id']
        _input_amount = _json['amount']
        _point_amount = _input_amount * 0.1	
        if _from_id and _to_id and _input_amount and request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)		
            sqlQuery_1 = """INSERT INTO tb_point(member_idx, point_amount, update_uttm, update_dt, create_uttm, earned_point) 
                            VALUES(%s, %s,UNIX_TIMESTAMP(NOW()), FROM_UNIXTIME(UNIX_TIMESTAMP(NOW())),UNIX_TIMESTAMP(NOW()), %s)
                            ON DUPLICATE KEY UPDATE point_amount = point_amount + %s, earned_point = earned_point + %s ;"""
            sqlQuery_2 = """INSERT INTO tb_point_history(point_code, point_amount, from_idx, to_idx, create_uttm, create_date, trans_type) 
                            VALUES(777, %s, %s, %s, UNIX_TIMESTAMP(NOW()), FROM_UNIXTIME(UNIX_TIMESTAMP(NOW())), 0);"""
            bindData_1 = (_to_id, _point_amount,_point_amount,_point_amount,_point_amount)
            bindData_2 = (_point_amount, _from_id, _to_id)     
            cursor.execute(sqlQuery_1, bindData_1)
            conn.commit()
            cursor.execute(sqlQuery_2, bindData_2)
            conn.commit()
            respone = jsonify('successfully UPDATED')
            respone.status_code = 200
            return respone
        else:
            return showMessage()
    except Exception as e:
        conn.rollback()
        print(e)
    finally:
        cursor.close() 
        conn.close()  

@application.route('/decide_parent', methods=['GET'])
def decide_parent():
    try:
        _json = request.json
        _from_id = _json['from_id']
        #_to_id = _json['to_id']
        
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        bindData = (_from_id)
        sqlQuery = """SELECT * FROM tb_org_chart where member_idx = %s """
        cursor.execute(sqlQuery, bindData)
        Rows = cursor.fetchone()
        p_id = Rows["parent_id"]
        respone = jsonify(p_id)
        respone.parent_decide = False
        respone.status_code = 200
        return respone
    except Exception as e:
        respone = jsonify("Failed to SELECT")
        respone.parent_decide = False
        respone.status_code = 200
    finally:
        
        cursor.close() 
        conn.close()  
        return respone

@application.route('/test_2', methods=['POST'])
def end_mining():
    try:        
        _json = request.json
        _mining_idx = _json["mining_idx"]
        

        if _mining_idx and request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            	
            sqlQuery_0 = """SELECT * FROM tb_mining_rel x
                            INNER JOIN tb_robot y
                            ON y.robot_idx = x.robot_idx
                            WHERE x.mining_idx = %s;"""
                                        
            sqlQuery_1 = """UPDATE tb_mining
                            SET end_ut = UNIX_TIMESTAMP(NOW()),
                                mineral_amount = %s,
                                total_working_time = %s
                            WHERE mining_idx = %s;"""
            sqlQuery_2 = """UPDATE tb_robot
                            SET working_yn = 2,
                                update_dt = FROM_UNIXTIME(UNIX_TIMESTAMP(NOW())),
                                update_ut = UNIX_TIMESTAMP(NOW())
                            WHERE robot_idx = %s"""
            sqlQuery_3 = """INSERT INTO tb_mineral(member_idx, mineral_amount, mineral_from_mining, update_ut, update_dt) 
                            VALUES(%s, %s, %s,UNIX_TIMESTAMP(NOW()), FROM_UNIXTIME(UNIX_TIMESTAMP(NOW())))
                            ON DUPLICATE KEY UPDATE mineral_amount = mineral_amount + %s, mineral_from_mining = mineral_from_mining + %s ;"""
            sqlQuery_4 = """INSERT INTO tb_mineral_history(member_idx, mineral_chg_amount, from_member_idx, create_ut, create_dt, type) 
                            VALUES(%s, %s, %s, UNIX_TIMESTAMP(NOW()), FROM_UNIXTIME(UNIX_TIMESTAMP(NOW())), %s);"""


            bindData_0 = (_mining_idx)
            cursor.execute(sqlQuery_0,bindData_0)
            Rows = cursor.fetchone()
            
            
            _start_ut = Rows["start_ut"]
            _working_yn = Rows["working_yn"]
            _robot_member_idx = Rows["robot_member_idx"]
            _robot_idx = Rows["robot_idx"]

            if _working_yn == 1:
                delta_time = int(time.time()) - _start_ut
                if delta_time >= 72000: 
                    
                    
                    bindData_1 = (20, delta_time, _mining_idx)    
                    bindData_2 = (_robot_idx)
                    bindData_3 = (_robot_member_idx, 20, 20, 20, 20)
                    bindData_4 = (_robot_member_idx, 20, _robot_member_idx,0)
                    cursor.execute(sqlQuery_1, bindData_1)
                    conn.commit()
                    cursor.execute(sqlQuery_2, bindData_2)
                    conn.commit()
                    cursor.execute(sqlQuery_3, bindData_3)
                    conn.commit()
                    cursor.execute(sqlQuery_4, bindData_4)
                    conn.commit()
                    
                else:
                    t_m = 20 * (delta_time/72000)
                    bindData_1 = (t_m, delta_time, _mining_idx)    
                    bindData_2 = (_robot_idx)
                    bindData_3 = (_robot_member_idx, t_m, t_m, t_m, t_m)
                    bindData_4 = (_robot_member_idx, t_m, _robot_member_idx,0)
                    cursor.execute(sqlQuery_1, bindData_1)
                    conn.commit()
                    cursor.execute(sqlQuery_2, bindData_2)
                    conn.commit()
                    cursor.execute(sqlQuery_3, bindData_3)
                    conn.commit()
                    cursor.execute(sqlQuery_4, bindData_4)
                    conn.commit()

            else:
                respone = jsonify('This robot is not working')
                respone.status_code = 200
                
            respone = jsonify('successfully UPDATED')
            respone.status_code = 200
            return respone
            
        else:
            return showMessage()
    except Exception as e:
        conn.rollback()
        respone = jsonify('ERROR!!!')
        respone.status_code = 200
        print(e)
    finally:
        cursor.close() 
        conn.close()  
        return respone


@application.route('/test_1', methods=['POST'])
def start_mining():
    try:        
        _json = request.json
        _robot_idx = _json['robot_idx']
        	
        	
        if _robot_idx and request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            	
            sqlQuery_0 = """SELECT * FROM tb_robot x
                            RIGHT JOIN tb_land y
                            ON x.robot_member_idx = y.owner_idx 
                            WHERE x.robot_idx = %s"""
            
            sqlQuery_1 = """INSERT INTO tb_mining(member_idx, robot_idx, start_dt, start_ut) 
                            VALUES(%s, %s, FROM_UNIXTIME(UNIX_TIMESTAMP(NOW())),UNIX_TIMESTAMP(NOW()))
                            ;"""

            sqlQuery_1_1 = """SELECT LAST_INSERT_ID() as mining_idx"""
            sqlQuery_2 = """INSERT INTO tb_mining_rel(member_idx, robot_idx, mining_idx, start_ut, land_idx) 
                            VALUES(%s, %s, %s, UNIX_TIMESTAMP(NOW()), %s);"""
            sqlQuery_3 = """UPDATE tb_robot
                            SET working_yn = 1,
                                update_dt = FROM_UNIXTIME(UNIX_TIMESTAMP(NOW())),
                                update_ut = UNIX_TIMESTAMP(NOW())
                            WHERE robot_idx = %s ;"""
            bindData_0 = (_robot_idx)
            cursor.execute(sqlQuery_0, bindData_0)
            Rows = cursor.fetchone()

            _robot_member_idx = Rows['robot_member_idx']
            _working_yn = Rows['working_yn']
            _update_ut = Rows['update_ut']
            _land_idx = Rows["land_idx"]
            

            bindData_1 = (_robot_member_idx, _robot_idx)
            
            bindData_3 = (_robot_idx)
            if _working_yn == 2:
                time_gap = time.time()- _update_ut
                if time_gap > 14400:
                    cursor.execute(sqlQuery_1, bindData_1)
                    conn.commit()
                    cursor.execute(sqlQuery_1_1)
                    Rows_1 = cursor.fetchone()
                    _mining_idx = Rows_1["mining_idx"]
                    bindData_2 = (_robot_member_idx, _robot_idx, _mining_idx, _land_idx)     
                    cursor.execute(sqlQuery_2, bindData_2)
                    conn.commit()
                    cursor.execute(sqlQuery_3, bindData_3)
                    conn.commit()
                    respone = jsonify('successfully INSULT ')
                    respone.status_code = 200
                    
                else:
                    respone = jsonify('can not start to mining. you need to maintanence your robot')
                    respone.status_code = 200
                    return respone
            elif _working_yn == 0:
                
                cursor.execute(sqlQuery_1, bindData_1)
                conn.commit()
                cursor.execute(sqlQuery_1_1)
                Rows_1 = cursor.fetchone()
                _mining_idx = Rows_1["mining_idx"]
                bindData_2 = (_robot_member_idx, _robot_idx, _mining_idx, _land_idx)   
                cursor.execute(sqlQuery_2, bindData_2)
                conn.commit()
                cursor.execute(sqlQuery_3, bindData_3)
                conn.commit()
                respone = jsonify('successfully INSULT ')
                respone.status_code = 200
            else:

                respone = jsonify('This robot is already working')
                respone.status_code = 200
                

        else:
            return showMessage()
    except Exception as e:
        conn.rollback()
        respone = respone = jsonify('ERROR ')
        respone.status_code = 200
        print(e)
    finally:
        cursor.close() 
        conn.close()  
        return respone

@application.route('/getWallet', methods=['POST'])
async def test():

    client = AsyncClient("https://api.mainnet-beta.solana.com")   
    account = Keypair().generate()
    
    await client.close()
    #respone = jsonify('successfully UPDATED, pubkey is ')
    #respone.status_code = 200
    print("step1 ok")

    try:
        _wallet_pubkey = account.public_key
        _wallet_seckey = account.secret_key
        print(_wallet_pubkey)
        print(_wallet_seckey)
        code = BitArray(bytes = _wallet_seckey)
        string_code = str(code)

        new_pubkey = str(_wallet_pubkey)
        new_seckey = string_code[2:]
        massage = {
                'status' : "Y",
                'message' : 'Successfully create wallet',
                'pubkey' : new_pubkey,
                'seckey' : new_seckey
                }
        respone = jsonify(massage)
        respone.status_code = 200
        return respone
    except Exception as e:
        massage = {
                'status' : "N",
                'message' : 'Failed to create wallet'
            }
        respone = jsonify(massage)
        respone.status_code = 200
        #conn.rollback()
        print(e)
    finally:
        
        #cursor.close() 
        #conn.close()  
        return respone

@application.errorhandler(404)
def showMessage(error=None):
    message = {
        'status': 404,
        'message': 'Record not found: ' + request.url,
    }
    respone = jsonify(message)
    respone.status_code = 404
    return respone
        
if __name__ == "__main__":
    application.run()