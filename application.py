from email import message
from email.mime import application
from tkinter.messagebox import NO
from urllib import response
import pymysql
from flask import jsonify
from flask import Flask
from flask import request
from flaskext.mysql import MySQL
from flask_cors import CORS, cross_origin
from solana.rpc.async_api import AsyncClient
from solana.keypair import Keypair
import time
import requests
import json
from bitstring import BitArray
import ast
from eth_account import Account
import secrets
import asyncio
from solana.publickey import PublicKey
from pytz import timezone
import datetime

application = Flask(__name__)
CORS(application)
mysql = MySQL()
application.config['MYSQL_DATABASE_USER'] = 'memorics'
application.config['MYSQL_DATABASE_PASSWORD'] = 'qwer12#$'
application.config['MYSQL_DATABASE_DB'] = 'msx'
application.config['MYSQL_DATABASE_HOST'] = 'p2pservice-rds-prod.cf219cl1yo7q.ap-northeast-2.rds.amazonaws.com'
application.config['MYSQL_DATABASE_PORT'] = 3306
mysql.init_app(application)


@application.route('/', methods=['POST','GET'])
def root():
    try:        
        
        respone = jsonify('successfully UPDATED')
        respone.status_code = 200
        
        
    except Exception as e:
        respone = jsonify('there is an error')
        return respone
    finally:
        return respone  

@application.route('/decide_title', methods=['POST'])
def decide_title():
    try:        
        _json = request.json
        _member_idx = _json['member_idx']
        
        if _member_idx and request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)		
            sqlQuery_0 = """SELECT * FROM tb_org_chart AS a
                            JOIN tb_land AS b
                            ON a.member_idx = b.owner_idx
                            WHERE a.member_idx = %s;
                            """
            sqlQuery_1 = """SELECT * FROM (SELECT * FROM (WITH RECURSIVE cte (member_idx, member_id, parent_idx , title ,ord, lvl) AS (
                            SELECT     member_idx,
                                        member_id,
                                        parent_idx,
                                        title,
                                        ord,
                                        1 as lvl
                            FROM       tb_org_chart
                            WHERE      member_idx = %s
                            UNION ALL
                            SELECT     p.member_idx,
                                        p.member_id,
                                        p.parent_idx,
                                        p.title,
                                        p.ord,
                                        x.lvl + 1 lvl
                            FROM       tb_org_chart p
                            INNER JOIN cte x
                                    on p.parent_idx = x.member_idx
                            )
                            SELECT * FROM cte 
                            WHERE lvl <= %s) a
                            JOIN tb_land b
                            ON a.member_idx = b.owner_idx
                            ORDER BY a.member_idx DESC) c 
                            JOIN tb_info_packages d
                            on c.refiner_lv = d.refiner_type
                            WHERE c.member_idx NOT IN (%s);"""
            sqlQuery_2 = """UPDATE tb_org_chart
                            SET title = %s
                            WHERE member_idx = %s;
                            """
            sqlQuery_3 = """SELECT max(title) FROM (WITH RECURSIVE cte (member_idx, member_id, parent_idx , title ,ord, lvl) AS (
                            SELECT     member_idx,
                                        member_id,
                                        parent_idx,
                                        title,
                                        ord,
                                        1 as lvl
                            FROM       tb_org_chart
                            WHERE      member_idx = %s
                            UNION ALL
                            SELECT     p.member_idx,
                                        p.member_id,
                                        p.parent_idx,
                                        p.title,
                                        p.ord,
                                        x.lvl + 1 lvl
                            FROM       tb_org_chart p
                            INNER JOIN cte x
                                    on p.parent_idx = x.member_idx
                            )
                            SELECT * FROM cte 
                            WHERE lvl <= %s) a
                            JOIN tb_land b
                            ON a.member_idx = b.owner_idx
                            ORDER BY a.member_idx asc;
                            """
            bindData_0 = (_member_idx)
            cursor.execute(sqlQuery_0, bindData_0)
            result_1 = cursor.fetchone()
            influence_lv = result_1["influence_lv"]
            bindData_1 = (_member_idx, influence_lv+1, _member_idx) 
            
            cursor.execute(sqlQuery_1, bindData_1)
            result_2 = cursor.fetchall()


            num_of_ref = 0
            total_pack_price = 0
            for row in result_2:
                if row["lvl"] <= influence_lv:
                    if row["lvl"] == 2:
                        num_of_ref = num_of_ref + 1
                    total_pack_price = total_pack_price + row["packages_price"]
                    print("total package price is ", total_pack_price)
                    print("number or referral is ", num_of_ref)

            if result_1["title"] == 0:
                #추천인이 5인이 넘는지 (단 랜드 구매자한함.)
                
                if num_of_ref >= 3 and total_pack_price >= 5000:
                    bindData_2 = (1, _member_idx)  
                    cursor.execute(sqlQuery_2,bindData_2)
                    messege = {
                        "amount" : 1,
                        "result_code" : 240,
                        "status" : 200
                    }
                    respone = jsonify(messege)
                    respone.status_code = 240
                else:
                    messege = {
                        "result_code" : 241,
                        "amount" : 0,
                        "status" : 200
                    }
                    respone = jsonify(messege)
                    respone.status_code = 241
            elif 7 > result_1["title"] >= 1:
                number_of_cond = 0
                print("the order of top level is ", num_of_ref -1)
                if num_of_ref >=3:
                    print("number or line is ", num_of_ref -1)
                    for i in range(num_of_ref -1):
                        
                        bindData_3 = (result_2[i]["member_idx"], influence_lv)
                        cursor.execute(sqlQuery_3,bindData_3)
                        bottom = cursor.fetchone()
                        if bottom["max(title)"] >= result_1["title"]:
                            number_of_cond =number_of_cond + 1
                        print("{} 님은 타이틀 래밸은 {} 입니다.".format(result_2[i]["member_idx"], bottom["max(title)"]))
                    print("조건을 만족하는 영지는 {} 개 있습니다.".format(number_of_cond))
                    if number_of_cond >= 3:
                        bindData_2 = (result_1["title"] + 1, _member_idx)  
                        cursor.execute(sqlQuery_2,bindData_2)
                        messege = {
                        "amount" : result_1["title"],
                        "result_code" : 240,
                        "status" : 200
                        }
                        respone = jsonify(messege)
                        respone.status_code = 240
                    else:
                        messege = {
                        "amount" : result_1["title"],
                        "result_code" : 241,
                        "status" : 200
                        }
                        respone = jsonify(messege)
                        respone.status_code = 241
                else:
                    messege = {
                    "amount" : result_1["title"],
                    "result_code" : 241,
                    "status" : 200
                    }
                    respone = jsonify(messege)
                    respone.status_code = 241
            elif result_1["title"] == 7:
                messege = {
                "amount" : result_1["title"],
                "result_code" : 242,
                "status" : 200
                }
                respone = jsonify(messege)
                respone.status_code = 242
        else:
            return showMessage()
    except Exception as e:
        conn.rollback()
        print(e)
        messege = {
                        "amount" : result_1["title"],
                        "result_code" : 243,
                        "status" : 500
                        }
        respone = jsonify(messege)
        respone.status_code = 243
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone

@application.route('/getWallet_sol', methods=['POST'])
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
                'status' : 200,
                'result_code' : 230,
                'pubkey' : new_pubkey,
                'seckey' : new_seckey
                }
        respone = jsonify(massage)
        respone.status_code = 230
        return respone
    except Exception as e:
        massage = {
                'status' : 500,
                'result_code' : 231,
                'pubkey' : None,
                'seckey' : None
            }
        respone = jsonify(massage)
        respone.status_code = 231
        #conn.rollback()
        print(e)
    finally:
        
        #cursor.close() 
        #conn.close()  
        return respone



@application.route('/getWallet_klay', methods=['GET','POST'])
async def getwallet_klay():
    try:
        url = "https://wallet-api.klaytnapi.com/v2/account"
        payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "klay_blockNumber",
        "params": [],
        "id": 73
        })
        headers = {
        'x-chain-id': '8217',
        'Authorization': 'Basic S0FTS1FTQjU4RVlONU1PTlFEOEFMUFdEOllQSDhieG5XeVd3eFJIVkt2VmR4aW1YM3M5Q0JEM3lLdHdtbXJ3Vno=',
        'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        #response.status_code = 200
        byte_str = response.content
        dict_str = byte_str.decode("UTF-8")
        mydata = ast.literal_eval(dict_str)
        sp_mydata = mydata["keyId"].split(':')[6]
        respone = jsonify({
            "pubkey" : mydata["address"],
            "seckey" : sp_mydata,
            "publicKey" : mydata["publicKey"],
            "status" : 200,
            "result_code": 230 
        })
        print(response.text)
    except Exception as e:
        
        respone = jsonify({
            "pubkey" : None,
            "seckey" : None,
            "publicKey" : None,
            "status" : 500,
            "result_code": 231 
        })
        respone.status_code = 231
        print(e)
    
    finally:
        return respone
    
@application.route('/getWallet_usdt', methods=['GET','POST'])
async def getwallet_usdt():
    try:
        priv = secrets.token_hex(32)
        private_key = "0x" + priv
        #print ("SAVE BUT DO NOT SHARE THIS:", private_key)
        acct = Account.from_key(private_key)
        #print("Address:", acct.address)
        respone = jsonify({
            "seckey" : private_key,
            "pubkey" : acct.address,
            "status" : "Y",
            "message": "Successfully create wallet"
        })
        respone.status_code = 200
    except Exception as e:
        
        respone =jsonify('ERROR ')
        respone.status_code = 200
        print(e)
    
    finally:
        return respone

@application.route('/check_klay', methods=['GET','POST'])
async def check_klay():
    try:
        _json = request.json
        _pubkey = _json["public_key"]
        

        url = "https://node-api.klaytnapi.com/v1/klaytn"
        payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "klay_getAccount",
        "params": [_pubkey, "latest"],
        "id": 1
        })
        headers = {
        'x-chain-id': '8217',
        'Authorization': 'Basic S0FTS1FTQjU4RVlONU1PTlFEOEFMUFdEOllQSDhieG5XeVd3eFJIVkt2VmR4aW1YM3M5Q0JEM3lLdHdtbXJ3Vno=',
        'Content-Type': 'application/json'
        }
        
        response = requests.request("POST", url, headers=headers, data=payload)
        #response.status_code = 200
        byte_str = response.content
        dict_str = byte_str.decode("utf-8")  
        print(dict_str)
        mydata = json.loads(dict_str)    
        if mydata["result"]:
            balance = (mydata["result"]["account"]["balance"])[2:]
            print(balance)
            up_bal_2 = ast.literal_eval(mydata["result"]["account"]["balance"])
            print(up_bal_2)
            print(round((up_bal_2)/(10^18)))
            respone = jsonify({
                "amount" : up_bal_2/1000000000000000000,
                'status' : 200,
                'result_code' : 232

            })
            respone.status_code = 232
        else:
            respone = jsonify({
                "amount" : 0,
                'status' : 200,
                'result_code' : 232

            })
            respone.status_code = 232
        
    except Exception as e:
        
        respone = jsonify({
            "amount" : 0,
            'status' : 500,
            'result_code' : 233

        })
        respone.status_code = 232
    
    finally:
        return respone
    
@application.route('/check_sol', methods=['POST'])
async def check_sol():

    _json = request.json
    _pubkey = _json["public_key"]
    client = AsyncClient("https://api.mainnet-beta.solana.com")   
    lamport = 0.000000001
    print("step0 ok")
    balance = await asyncio.create_task(client.get_balance(PublicKey(_pubkey),"finalized"))
    print(balance)

    try:
        
        
        massage = {
                'status' : 200,
                'result_code' : 232,
                "amount" : balance["result"]["value"] * lamport
                }
        respone = jsonify(massage)
        respone.status_code = 232

        await client.close()
        return respone
        
    except Exception as e:
        massage = {
                'status' : 500,
                'result_code' : 233,
                "amount" : 0
                }
        respone = jsonify(massage)
        respone.status_code = 233
        #conn.rollback()
        print(e)
    finally:
        
        #cursor.close() 
        #conn.close()  
        return respone

@application.route('/settlement', methods=['POST'])
def settlement():
    now_dt = datetime.datetime.now(timezone("Asia/Seoul"))
    now_uttm = int(round(datetime.datetime.now(timezone("Asia/Seoul")).timestamp()))
    #맴버 idx 까지 받아서 2차검증필요
    try:        
        _json = request.json
        _member_idx = _json['member_idx']

        if _member_idx and request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            sqlQuery_0 = """SELECT * FROM tb_mineral AS a
                            INNER JOIN tb_land AS b
                            INNER JOIN tb_org_chart AS c
                            ON a.member_idx = b.owner_idx AND a.member_idx = c.member_idx
                            WHERE a.member_idx = %s;
                            """
            sqlQuery_1 = """SELECT * FROM (WITH RECURSIVE cte (member_idx, member_id, parent_idx , title , lvl) AS (
                            SELECT     member_idx,
                                        member_id,
                                        parent_idx,
                                        title,
                                        1 as lvl
                            FROM       tb_org_chart
                            WHERE      member_idx = %s
                            UNION ALL
                            SELECT     p.member_idx,
                                        p.member_id,
                                        p.parent_idx,
                                        p.title,
                                        x.lvl + 1 lvl
                            FROM       tb_org_chart p
                            INNER JOIN cte x
                                    on p.member_idx = x.parent_idx
                            )
                            SELECT * FROM cte 
                            LIMIT 31) a
                            JOIN tb_land b
                            ON a.member_idx = b.owner_idx
                            ORDER BY a.member_idx DESC;"""
            bindData_0 = (_member_idx)
            bindData_1 = (_member_idx)
            
            cursor.execute(sqlQuery_0,bindData_0)
            user_info = cursor.fetchone()
            cursor.execute(sqlQuery_1,bindData_1)
            Rows = cursor.fetchall()
            #Rows = 해당 맴버 상위로 30개 를 가져온다.
            result_list_1 = []
            result_list_2 = []
            result_list_3 = [] #
            if Rows and user_info:
                if Rows[0]["lvl"] == 1:
                    sqlQuery_s = """select nextval(sq_group_stl)"""
                    cursor.execute(sqlQuery_s)
                    group_idx = cursor.fetchone()["nextval(sq_group_stl)"]
                    print("group idx is ", group_idx)
                    _mineral_start = user_info["mineral_amount"] 
                    _mineral_amount = user_info["mineral_amount"]*0.7
                    _title = user_info["title"]
                    _ref_enery = user_info["refining_energy"]
                
                    if _ref_enery >= _mineral_amount:
                        effective_mineral = _mineral_amount
                    else:
                        effective_mineral = _ref_enery
                    if effective_mineral != 0:
                    # 미네랄에서 새금을 제외한다. 만약 남은 re 보다 많다면 그대로 정산되고, re가 부족하다면 남은 re 만큼 정산된다.
                        for r in Rows:
                            if r["influence_lv"] >= r["lvl"]-1: #해당 로우의 유저의 인플루언스 래밸(수당래밸) 이 발생자와의 거리보다 크거나 같아야지 수당을 받을수있다.
                                if r["title"] >= (_title): #해당 로우의 유저의 타이틀이 발생자보다 같거나 높으면 정가로, 아니면 반값
                                    refined_mineral = effective_mineral
                                else:
                                    refined_mineral = effective_mineral * 0.5
                                
                                if r["lvl"]-1 == 0:
                                    refined_mineral = refined_mineral
                                elif r["lvl"]-1 == 1:
                                    refined_mineral = refined_mineral*1
                                elif r["lvl"]-1 == 2:
                                    refined_mineral = refined_mineral*0.4
                                elif r["lvl"]-1 == 3:
                                    refined_mineral = refined_mineral*0.3
                                elif r["lvl"]-1 == 4:
                                    refined_mineral = refined_mineral*0.2
                                elif r["lvl"]-1 == 5:
                                    refined_mineral = refined_mineral*0.15
                                elif 10 >= r["lvl"]-1 >= 6:
                                    refined_mineral = refined_mineral*0.05
                                elif 15 >= r["lvl"]-1 >= 11:
                                    refined_mineral = refined_mineral*0.03
                                elif 20 >= r["lvl"]-1 >= 16:
                                    refined_mineral = refined_mineral*0.01
                                elif 30 >= r["lvl"]-1 >= 21:
                                    refined_mineral = refined_mineral*0.005
                                
                                r["refined_mineral"] = refined_mineral
                                
                                temp_tuple_1 = (r["refined_mineral"], now_dt , r["refined_mineral"] ,r["member_idx"])
                                temp_tuple_2 = (r["member_idx"], _member_idx, "settlement", r["refined_mineral"], now_uttm, now_dt, group_idx)
                                temp_tuple_3 = (r["refined_mineral"], r["refined_mineral"], now_dt, r["member_idx"])
                                result_list_1.append(temp_tuple_1)
                                result_list_2.append(temp_tuple_2)
                                result_list_3.append(temp_tuple_3)

                        sqlQuery_1 = """INSERT INTO tb_mineral(member_idx, mineral_amount, update_dt, update_uttm)
                                    VALUES(%s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE mineral_amount = mineral_amount + %s,
                                                            update_dt = %s,
                                                            update_uttm = %s;"""
                        sqlQuery_2 = """INSERT INTO tb_mineral_history(member_idx, mineral_chg_amount, from_member_idx, create_dt, create_uttm, type) 
                                    VALUES(%s, %s, %s, %s, %s, %s);"""
                        sqlQuery_3 = """UPDATE tb_point
                                        SET point = point + %s,
                                            mod_dt = %s,
                                            get_point = get_point + %s
                                        WHERE member_idx = %s
                                                            ;"""
                        sqlQuery_4 = """INSERT INTO tb_point_history(member_idx, send_member_idx, point_cd, point, create_ut, create_dt, grp_idx) 
                                    VALUES(%s, %s, %s, %s, %s, %s, %s);"""
                        sqlQuery_5 = """UPDATE tb_land
                                        SET refining_energy = CASE WHEN refining_energy - %s < 0 THEN 0 ELSE refining_energy - %s END,
                                            update_dt = %s
                                        WHERE owner_idx = %s"""

                        print("이까지 ㅇㅋㄴ")
                        bindData_1 = (_member_idx, _mineral_start*(-1), now_dt, now_uttm, _mineral_start*(-1), now_dt, now_uttm)
                        bindData_2 = (_member_idx, _mineral_start*(-1), _member_idx, now_dt, now_uttm, 2)
                        bindData_3 = result_list_1
                        bindData_4 = result_list_2
                        bindData_5 = result_list_3
                        cursor.execute(sqlQuery_1,bindData_1)
                        print("이까지 ㅇㅋ1")
                        cursor.execute(sqlQuery_2,bindData_2)
                        print("이까지 ㅇㅋ2")
                        print("bindDate3 는 ",bindData_3)
                        print("bindDate4 는 ",bindData_4)
                        cursor.executemany(sqlQuery_4,bindData_4)
                        print("이까지 ㅇㅋ4")
                        cursor.executemany(sqlQuery_3,bindData_3)
                        print("이까지 ㅇㅋ3")
                        cursor.executemany(sqlQuery_5,bindData_5)
                        message = {
                        "status" : 200,
                        "amount" : effective_mineral,
                        "result_code" : 220
                        }
                        respone = jsonify(message)
                        respone.status_code = 220 
                    else:
                        message = {
                        "status" : 200,
                        "amount" : 0,
                        "result_code" : 221
                        }
                        respone = jsonify(message)
                        respone.status_code = 221
                else:
                    message = {
                        "status" : 200,
                        "amount" : 0,
                        "result_code" : 222
                        }
                    respone = jsonify(message)
                    respone.status_code = 222
            else:
                message = {
                    "status" : 200,
                    "amount" : 0,
                    "result_code" : 223
                }
                respone = jsonify(message)
                respone.status_code = 223
    except Exception as e:
        conn.rollback()
        message = {
                    "status" : 500,
                    "amount" : 0,
                    "result_code" : 224
        }
        respone = jsonify(message)
        respone.status_code = 224
        print(e)
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone

@application.route('/end_mining_all', methods=['POST'])
def total_end_mining():
    try:
        _json = request.json
        _member_idx = _json["member_idx"]

        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        sqlQuery_0 = """SELECT * FROM tb_mining a
                        JOIN tb_land b
                        ON a.member_idx = b.owner_idx
                        WHERE member_idx = %s"""
        bindData_0 = (_member_idx)
        sqlQuery_1 = """UPDATE tb_mining_history a , tb_mining b
                                SET a.end_uttm = %s,
                                    a.end_dt = %s,
                                    a.total_working_time = %s,
                                    a.mineral_amount = CASE WHEN (%s > 72000) THEN 20 ELSE (%s) END
                                WHERE (a.group_idx = b.group_idx AND a.member_idx = %s );
                                    """
                
        sqlQuery_1_1 = """SELECT sum(a.mineral_amount) x FROM tb_mining_history AS a
                        INNER JOIN tb_mining AS b
                        ON (a.robot_idx = b.robot_idx)
                        WHERE a.group_idx = b.group_idx AND a.member_idx = %s;"""

        sqlQuery_2 = """DELETE FROM tb_mining 
                        WHERE member_idx = %s;"""

        sqlQuery_3 = """UPDATE tb_robot x
                        SET x.working_yn = 0,
                            x.update_dt = %s,
                            x.update_uttm = %s,
                            x.total_amount = CASE WHEN (%s > 72000) THEN x.total_amount + 20 ELSE x.total_amount + %s END
                
                        WHERE x.working_yn = 1 AND x.robot_member_idx = %s"""
        
        sqlQuery_4 = """INSERT INTO tb_mineral(member_idx, mineral_amount, mineral_from_mining, update_dt, update_uttm)
                        VALUES(%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE mineral_amount = mineral_amount + %s,
                                                mineral_from_mining = mineral_from_mining + %s,
                                                update_dt = %s,
                                                update_uttm = %s"""
        sqlQuery_5 = """INSERT INTO tb_mineral_history(member_idx, mineral_chg_amount, from_member_idx, create_dt, create_uttm, type) 
                        VALUES(%s, %s, %s, %s ,%s,%s);
                        """
        cursor.execute(sqlQuery_0,bindData_0)
        Rows = cursor.fetchall()
        if Rows :
            if Rows[0]["land_type"] == 0:
                cnt_robot = len(Rows)
                
                now_dt = datetime.datetime.now(timezone("Asia/Seoul"))
                now_uttm = int(round(datetime.datetime.now(timezone("Asia/Seoul")).timestamp()))
                start_uttm = Rows[0]["start_uttm"]
                mineral_plus = ((now_uttm -start_uttm)/72000)*20 
                
                                
                bindData_1 = (now_uttm, now_dt, now_uttm - start_uttm, now_uttm - start_uttm, mineral_plus, _member_idx)
                bindData_1_1 = (_member_idx)
                bindData_2 = (_member_idx)
                bindData_3 = (now_dt, now_uttm, now_uttm - start_uttm, mineral_plus , _member_idx)
                
                cursor.execute(sqlQuery_1,bindData_1)
                cursor.execute(sqlQuery_1_1,bindData_1_1)
                temp_row = cursor.fetchone()
                mineral_insert = temp_row["x"]
                bindData_4 = (_member_idx, mineral_insert, mineral_insert, now_dt, now_uttm , mineral_insert, mineral_insert, now_dt, now_uttm)
                bindData_5 = (_member_idx, mineral_insert, _member_idx, now_dt, now_uttm, 0)
                cursor.execute(sqlQuery_3,bindData_3)
                cursor.execute(sqlQuery_4,bindData_4)
                cursor.execute(sqlQuery_5,bindData_5)
                cursor.execute(sqlQuery_2,bindData_2)

                message = {
                    "status" : 200,
                    "amount" : cnt_robot,
                    "result_code" : 210
                }
                respone = jsonify(message)
                respone.status_code = 210
            elif Rows[0]["land_type"] == 1:
                cnt_robot = len(Rows)
                
                now_dt = datetime.datetime.now(timezone("Asia/Seoul"))
                now_uttm = int(round(datetime.datetime.now(timezone("Asia/Seoul")).timestamp()))
                start_uttm = Rows[0]["start_uttm"] ##애매함
                mineral_plus = ((now_uttm -start_uttm)/72000)*2.5 
                sqlQuery_1 = """UPDATE tb_mining_history a , tb_mining b
                                SET a.end_uttm = %s,
                                    a.end_dt = %s,
                                    a.total_working_time = %s,
                                    a.mineral_amount = CASE WHEN (%s > 72000) THEN 2.5 ELSE (%s) END
                                WHERE (a.group_idx = b.group_idx AND a.member_idx = %s );
                                    """
                sqlQuery_3 = """UPDATE tb_robot x
                        SET x.working_yn = 0,
                            x.update_dt = %s,
                            x.update_uttm = %s,
                            x.total_amount = CASE WHEN (%s > 72000) THEN x.total_amount + 2.5 ELSE x.total_amount + %s END
                
                        WHERE x.working_yn = 1 AND x.robot_member_idx = %s"""      
                sqlQuery_4 = """INSERT INTO tb_mineral(member_idx, mineral_amount, mineral_from_mining, update_dt, update_uttm)
                        VALUES(%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE mineral_amount = mineral_amount + %s,
                                                mineral_from_mining = mineral_from_mining + %s,
                                                update_dt = %s,
                                                update_uttm = %s"""          
                bindData_1 = (now_uttm, now_dt, now_uttm - start_uttm, now_uttm - start_uttm, mineral_plus, _member_idx)
                bindData_1_1 = (_member_idx)
                bindData_2 = (_member_idx)
                bindData_3 = (now_dt, now_uttm, now_uttm - start_uttm, mineral_plus , _member_idx)
                
                cursor.execute(sqlQuery_1,bindData_1)
                cursor.execute(sqlQuery_1_1,bindData_1_1)
                temp_row = cursor.fetchone()
                mineral_insert = temp_row["x"]
                bindData_4 = (_member_idx, mineral_insert, mineral_insert, now_dt, now_uttm , mineral_insert, mineral_insert, now_dt, now_uttm)
                bindData_5 = (_member_idx, mineral_insert, _member_idx, now_dt, now_uttm, 0)
                cursor.execute(sqlQuery_3,bindData_3)
                cursor.execute(sqlQuery_4,bindData_4)
                cursor.execute(sqlQuery_5,bindData_5)
                cursor.execute(sqlQuery_2,bindData_2)

                message = {
                    "status" : 200,
                    "amount" : cnt_robot,
                    "result_code" : 210
                }
                respone = jsonify(message)
                respone.status_code = 210             
        else:
            message = {
                "status" : 200,
                "amount" : 0,
                "result_code" : 211
            }
            respone = jsonify(message)
            respone.status_code = 211
    except Exception as e:
        conn.rollback()
        message = {
                "status" : 500,
                "amount" : 0,
                "result_code" : 212
            }
        respone = jsonify(message)
        respone.status_code = 212
        print(e)
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone

@application.route('/start_mining_all', methods=['POST'])
def total_start_mining():
    #맴버 idx 까지 받아서 2차검증필요
    try:        
        _json = request.json
        _member_idx = _json['member_idx']
        
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
            
        sqlQuery_0 = """SELECT * FROM tb_robot
                        WHERE (robot_member_idx = %s 
                        AND working_yn = 0)"""
        bindData_0 = (_member_idx)
        
        cursor.execute(sqlQuery_0,bindData_0)
        Rows = cursor.fetchall()
        if Rows :
            cnt_robot = len(Rows)
            init_idx = Rows[0]["robot_idx"]
            start_dt = datetime.datetime.now(timezone("Asia/Seoul"))
            start_uttm = int(round(datetime.datetime.now(timezone("Asia/Seoul")).timestamp()))
            
            tb_mining_list = []
            tb_mining_history_list = []
            group_idx = str(init_idx)+str(int(round(datetime.datetime.now(timezone("Asia/Seoul")).timestamp())))+str(_member_idx)
            print("for 문 전 ")
            print(group_idx)
            for r in Rows:
                #print(type(r))
                _robot_idx = r["robot_idx"]   
                _land_idx = r["land_idx"]

                tb_mining_list.append((_member_idx,_robot_idx,start_dt,start_uttm,group_idx))
                
                tb_mining_history_list.append((_member_idx,_robot_idx,group_idx,start_dt,start_uttm,_land_idx))
                    
            sqlQuery_1 = """INSERT INTO tb_mining(member_idx, robot_idx, start_dt, start_uttm, group_idx) 
                                VALUES(%s, %s, %s, %s ,%s)
                                ;"""
            sqlQuery_2 = """INSERT INTO tb_mining_history(member_idx, robot_idx, group_idx, start_dt, start_uttm, land_idx) 
                                VALUES(%s, %s, %s, %s, %s, %s);"""

            sqlQuery_3 = """UPDATE tb_robot
                            SET working_yn = 1,
                                update_dt = %s,
                                update_uttm = %s
                            WHERE working_yn = 0 AND robot_member_idx = %s"""
            cursor.executemany(sqlQuery_1,tb_mining_list)
            cursor.executemany(sqlQuery_2,tb_mining_history_list)
            cursor.execute(sqlQuery_3,(start_dt, start_uttm, _member_idx))

            message = {
                "status" : 200,
                "amount" : cnt_robot,
                "result_code" : 213
            }
            respone = jsonify(message)
            respone.status_code = 213
        else:
            message = {
                "status" : 200,
                "amount" : 0,
                "result_code" : 214
            }
            respone = jsonify(message)
            respone.status_code = 214
    except Exception as e:
        conn.rollback()
        message = {
                "status" : 500,
                "amount" : 0,
                "result_code" : 215
            }
        respone =jsonify(message)
        respone.status_code = 215
        print(e)
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone


@application.route('/end_mining', methods=['POST'])
def end_mining():
    try:
        _json = request.json
        _member_idx = _json["member_idx"]
        _robot_idx = _json["robot_idx"]

        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        sqlQuery_0 = """SELECT * FROM tb_mining AS a
                        INNER JOIN tb_robot AS b
                        ON a.robot_idx = b.robot_idx
                        WHERE member_idx = %s AND a.robot_idx = %s"""
        bindData_0 = (_member_idx,_robot_idx)

        cursor.execute(sqlQuery_0,bindData_0)
        Rows = cursor.fetchone()
        if Rows :
            
            now_dt = datetime.datetime.now(timezone("Asia/Seoul"))
            now_uttm = int(round(datetime.datetime.now(timezone("Asia/Seoul")).timestamp()))
            start_uttm = Rows["start_uttm"]
            selected_group_idx = Rows["group_idx"]
            robot_name = Rows["robot_name"]
            mineral_plus = (now_uttm -start_uttm)/72000*20 

            sqlQuery_1 = """UPDATE tb_mining_history
                            SET end_uttm = %s,
                                end_dt = %s,
                                total_working_time = %s - start_uttm,
                                mineral_amount = CASE WHEN (%s - %s > 72000) THEN 20 ELSE (%s) END
                            WHERE (group_idx = %s AND member_idx = %s AND robot_idx = %s );
                                """
            sqlQuery_1_1 = """SELECT (mineral_amount) FROM tb_mining_history
                            WHERE group_idx = %s AND robot_idx = %s;"""

            sqlQuery_2 = """DELETE FROM tb_mining 
                            WHERE member_idx = %s AND group_idx = %s;"""

            sqlQuery_3 = """UPDATE tb_robot x
                            SET x.working_yn = 0,
                                x.update_dt = %s,
                                x.update_uttm = %s,
                                x.total_amount = CASE WHEN (%s - x.update_uttm > 72000) THEN x.total_amount + 20 ELSE x.total_amount + %s END
                    
                            WHERE x.robot_member_idx = %s AND x.robot_idx = %s"""
            sqlQuery_4 = """INSERT INTO tb_mineral(member_idx, mineral_amount, mineral_from_mining, update_dt, update_uttm)
                            VALUES(%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE mineral_amount = mineral_amount + %s,
                                                    mineral_from_mining = mineral_from_mining + %s,
                                                    update_dt = %s,
                                                    update_uttm = %s"""
            
         

            sqlQuery_5 = """INSERT INTO tb_mineral_history(member_idx, mineral_chg_amount, from_member_idx, create_dt, create_uttm, type) 
                            VALUES(%s, %s, %s, %s ,%s ,%s);
                            """
                            
            bindData_1 = (now_uttm, now_dt, now_uttm, now_uttm, start_uttm, mineral_plus ,selected_group_idx, _member_idx, _robot_idx)
            bindData_1_1 = (selected_group_idx, _robot_idx)
            bindData_2 = (_member_idx, selected_group_idx)
            bindData_3 = (now_dt, now_uttm, now_uttm, mineral_plus ,_member_idx, _robot_idx)
            
            cursor.execute(sqlQuery_1,bindData_1)
            cursor.execute(sqlQuery_1_1,bindData_1_1)
            temp_row = cursor.fetchone()
            mineral_insert = temp_row["mineral_amount"]
            bindData_4 = (_member_idx,mineral_insert, mineral_insert, now_dt, now_uttm , mineral_insert, mineral_insert, now_dt, now_uttm)
            bindData_5 = (_member_idx, mineral_insert,_member_idx, now_dt, now_uttm, 0)
            cursor.execute(sqlQuery_3,bindData_3)
            cursor.execute(sqlQuery_4,bindData_4)
            cursor.execute(sqlQuery_2,bindData_2)
            cursor.execute(sqlQuery_5,bindData_5)


            respone = jsonify("Success to end minining of robot_name : {0}".format(robot_name))
            respone.status_code = 200
        else:
            respone = jsonify("This robot is not mining mining.")

    except Exception as e:
        conn.rollback()
        respone =jsonify('ERROR ')
        respone.status_code = 200
        print(e)
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone

@application.route('/start_mining', methods=['POST'])
def start_mining():
    #맴버 idx 까지 받아서 2차검증필요
    try:        
        _json = request.json
        _robot_idx = _json['robot_idx']
        _member_idx = _json['member_idx']

        
        	
        if _robot_idx and _member_idx and request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            sqlQuery_0 = """SELECT * FROM tb_robot
                        WHERE (robot_member_idx = %s 
                        AND working_yn = 0 AND robot_idx = %s)"""
            bindData_0 = (_member_idx, _robot_idx)
            cursor.execute(sqlQuery_0,bindData_0)
            Rows = cursor.fetchone()

            if Rows:
                cnt_robot = len(Rows)
                init_idx = Rows["robot_idx"]
                land_idx = Rows['land_idx']
                robot_name = Rows["robot_name"]
                now_dt = datetime.datetime.now(timezone("Asia/Seoul"))
                now_uttm = int(round(datetime.datetime.now(timezone("Asia/Seoul")).timestamp()))
                group_idx = str(init_idx)+str(int(round(datetime.datetime.now(timezone("Asia/Seoul")).timestamp())))+str(_member_idx)
                
                sqlQuery_1 = """INSERT INTO tb_mining(member_idx, robot_idx, start_dt, start_uttm, group_idx) 
                                    VALUES(%s, %s, %s, %s ,%s)
                                    ;"""
                sqlQuery_2 = """INSERT INTO tb_mining_history(member_idx, robot_idx, group_idx, start_dt, start_uttm, land_idx) 
                                    VALUES(%s, %s, %s, %s, %s, %s);"""

                sqlQuery_3 = """UPDATE tb_robot
                                SET working_yn = 1,
                                    update_dt = %s,
                                    update_uttm = %s
                                WHERE working_yn = 0 AND robot_member_idx = %s AND robot_idx = %s"""
                
                bindData_1 = (_member_idx,_robot_idx,now_dt,now_uttm,group_idx)
                bindData_2 = (_member_idx,_robot_idx,group_idx,now_dt,now_uttm,land_idx)
                bindData_3 = (now_dt, now_uttm, _member_idx, _robot_idx)
                cursor.execute(sqlQuery_1,bindData_1)
                cursor.execute(sqlQuery_2,bindData_2)
                cursor.execute(sqlQuery_3,bindData_3)

                respone = jsonify('successfully start to mining robot {0}'.format(robot_name))
                respone.status_code = 200
            else:
                respone = jsonify("there is no such robot")
    except Exception as e:
        conn.rollback()
        respone = jsonify('ERROR ')
        respone.status_code = 200
        print(e)
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
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