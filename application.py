import pymysql
from flask import jsonify
from flask import Flask
from flask import request
from flaskext.mysql import MySQL
from flask_cors import CORS, cross_origin
from solana.rpc.async_api import AsyncClient
from solana.keypair import Keypair
import requests
import json
from bitstring import BitArray
import ast
import asyncio
from solana.publickey import PublicKey
from pytz import timezone
import datetime
from eth_account import Account
import secrets

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
async def root():
    try:        
        
        respone = jsonify('successfully UPDATED')
        respone.status_code = 200
        
        
    except Exception as e:
        respone = jsonify('there is an error')
        return respone
    finally:
        return respone  


def influnece_lv_counter(ref_count, influence_lv):
    if influence_lv <= 7:
        return min(ref_count,influence_lv)
    elif influence_lv == 10:
        if ref_count <= 7:
            return ref_count
        else:
            return influence_lv
    elif influence_lv == 13:
        if ref_count <= 7:
            return ref_count
        elif ref_count == 8:
            return 10
        else:
            return influence_lv
    else:
        if ref_count <= 7:
            return ref_count
        elif ref_count == 8:
            return 10
        elif ref_count == 9:
            return 13
        else:
            return influence_lv

@application.route('/decide_title', methods=['POST'])
async def decide_title():
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
            #해당 맴버의 정보를 서버에서 가져온다.
            influence_lv = result_1["influence_lv"] #패키지의 영향력 래밸
            
            cursor.execute("SELECT * FROM tb_member WHERE idx = {}".format(_member_idx))
            num_of_ref = (cursor.fetchone())["referral_regular_count"]
            real_inf_lv = influnece_lv_counter(num_of_ref,influence_lv)

            bindData_1 = (_member_idx, real_inf_lv+1, _member_idx) 
            cursor.execute(sqlQuery_1, bindData_1)
            result_2 = cursor.fetchall()
            
            #추천수
            total_pack_price = 0
            for row in result_2:
                if row["lvl"] <= real_inf_lv:
                    total_pack_price = total_pack_price + row["packages_price"]
                    print("total package price is ", total_pack_price)
                    print("number or referral is ", num_of_ref)
            
            
            if result_1["title"] == 0:
                #추천인이 3인이 넘는지 (단 랜드 구매자한함.)
                #승작성공하면 RM 을 지급하는거는 추가 개발 필요.
                if num_of_ref >= 3 and total_pack_price >= 5000:
                    bindData_2 = (1, _member_idx)  
                    cursor.execute(sqlQuery_2,bindData_2)
                    messege = {
                        "amount" : 1,
                        "result_code" : 240,
                        "status" : 200
                    }
                    respone = jsonify(messege)
                    respone.status_code = 200
                else:
                    messege = {
                        "result_code" : 241,
                        "amount" : 0,
                        "status" : 200
                    }
                    respone = jsonify(messege)
                    respone.status_code = 200
    
            elif 7 > result_1["title"] >= 1:
                number_of_cond = 0
                print("the order of top level is ", num_of_ref)
                if num_of_ref >=3:
                    print("number or line is ", num_of_ref)
                    for i in range(num_of_ref):
                        
                        bindData_3 = (result_2[i]["member_idx"], real_inf_lv +1)
                        cursor.execute(sqlQuery_3,bindData_3)
                        bottom = cursor.fetchone()
                        if bottom["max(title)"] >= result_1["title"]:
                            number_of_cond =number_of_cond + 1
                        print("{} 님은 타이틀 래밸은 {} 입니다.".format(result_2[i]["member_idx"], bottom["max(title)"]))
                    print("조건을 만족하는 영지는 {} 개 있습니다.".format(number_of_cond))
                    if number_of_cond >= 3:
                        if result_1["title"] != 1:
                            bindData_2 = (result_1["title"] + 1, _member_idx)  
                            cursor.execute(sqlQuery_2,bindData_2)
                            messege = {
                            "amount" : result_1["title"]+1,
                            "result_code" : 240,
                            "status" : 200
                            }
                            respone = jsonify(messege)
                            respone.status_code = 200
                        else:
                            if total_pack_price >= 130000:
                                bindData_2 = (result_1["title"] + 1, _member_idx)  
                                cursor.execute(sqlQuery_2,bindData_2)
                                messege = {
                                "amount" : result_1["title"]+1,
                                "result_code" : 240,
                                "status" : 200
                                }
                                respone = jsonify(messege)
                                respone.status_code = 200
                            else:
                                messege = {
                                "amount" : result_1["title"],
                                "result_code" : 241,
                                "status" : 200
                                }
                                respone = jsonify(messege)
                                respone.status_code = 200

                    else:
                        messege = {
                        "amount" : result_1["title"],
                        "result_code" : 241,
                        "status" : 200
                        }
                        respone = jsonify(messege)
                        respone.status_code = 200
                else:
                    messege = {
                    "amount" : result_1["title"],
                    "result_code" : 241,
                    "status" : 200
                    }
                    respone = jsonify(messege)
                    respone.status_code = 200
            elif result_1["title"] == 7:
                messege = {
                "amount" : result_1["title"],
                "result_code" : 242,
                "status" : 200
                }
                respone = jsonify(messege)
                respone.status_code = 200
        else:
            messege = {
                "amount" : 0,
                "result_code" : 248,
                "status" : 200
                }
            respone = jsonify(messege)
            respone.status_code = 200
    except Exception as e:
        conn.rollback()
        print(e)
        messege = {
                        "amount" : result_1["title"],
                        "result_code" : 243,
                        "status" : 500
                        }
        respone = jsonify(messege)
        respone.status_code = 200
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone


@application.route('/progress_rate', methods=['POST'])
async def progress_rate():
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
             
            
            cursor.execute("SELECT * FROM tb_member WHERE idx = {}".format(_member_idx))
            num_of_ref = (cursor.fetchone())["referral_regular_count"]
            real_inf_lv = influnece_lv_counter(num_of_ref,influence_lv)
            
            bindData_1 = (_member_idx, real_inf_lv+1, _member_idx)
            cursor.execute(sqlQuery_1, bindData_1)
            result_2 = cursor.fetchall()
            number_of_cond = 0

          
            total_pack_price = 0
            for row in result_2:
                total_pack_price = total_pack_price + row["packages_price"]
                print("total package price is ", total_pack_price)
                print("number or referral is ", num_of_ref)

            if result_1["title"] == 0:
                #추천인이 5인이 넘는지 (단 랜드 구매자한함.)
                
                if num_of_ref >= 3 and total_pack_price >= 5000:
                    messege = {
                        "amount" : 1,
                        "result_code" : 244,
                        "status" : 200,
                        "total_pack_price" : total_pack_price,
                        "num_of_ref" : num_of_ref,
                        "progress" : number_of_cond
                    }
                    respone = jsonify(messege)
                    respone.status_code = 200
                else:
                    messege = {
                        "result_code" : 245,
                        "amount" : 0,
                        "status" : 200,
                        "total_pack_price" : total_pack_price,
                        "num_of_ref" : num_of_ref,
                        "progress" : number_of_cond
                    }
                    respone = jsonify(messege)
                    respone.status_code = 200
            elif 7 > result_1["title"] >= 1:
                
                print("the order of top level is ", num_of_ref)
                if num_of_ref >=3:
                    print("number or line is ", num_of_ref)
                    for i in range(num_of_ref ):
                        
                        bindData_3 = (result_2[i]["member_idx"], influence_lv)
                        cursor.execute(sqlQuery_3,bindData_3)
                        bottom = cursor.fetchone()
                        if bottom["max(title)"] >= result_1["title"]:
                            number_of_cond =number_of_cond + 1
                        print("{} 님은 타이틀 래밸은 {} 입니다.".format(result_2[i]["member_idx"], bottom["max(title)"]))
                    print("조건을 만족하는 영지는 {} 개 있습니다.".format(number_of_cond))
                    if number_of_cond >= 3:
                        if result_1["title"] != 1:
                            messege = {
                            "amount" : result_1["title"],
                            "result_code" : 246,
                            "status" : 200,
                            "total_pack_price" : total_pack_price,
                            "num_of_ref" : num_of_ref,
                            "progress" : number_of_cond
                            }
                            respone = jsonify(messege)
                            respone.status_code = 200
                        else:
                            if total_pack_price >= 130000:
                                messege = {
                                "amount" : result_1["title"],
                                "result_code" : 246,
                                "status" : 200,
                                "total_pack_price" : total_pack_price,
                                "num_of_ref" : num_of_ref,
                                "progress" : number_of_cond
                                }
                                respone = jsonify(messege)
                                respone.status_code = 200
                            else:
                                messege = {
                                "amount" : result_1["title"],
                                "result_code" : 245,
                                "status" : 200,
                                "total_pack_price" : total_pack_price,
                                "num_of_ref" : num_of_ref,
                                "progress" : number_of_cond
                                }
                                respone = jsonify(messege)
                                respone.status_code = 200
                    else:
                        messege = {
                        "amount" : result_1["title"],
                        "result_code" : 245,
                        "status" : 200,
                        "total_pack_price" : total_pack_price,
                        "num_of_ref" : num_of_ref,
                        "progress" : number_of_cond
                        }
                        respone = jsonify(messege)
                        respone.status_code = 200
                else:
                    messege = {
                    "amount" : result_1["title"],
                    "result_code" : 245,
                    "status" : 200,
                    "total_pack_price" : total_pack_price,
                    "num_of_ref" : num_of_ref,
                    "progress" : number_of_cond
                    }
                    respone = jsonify(messege)
                    respone.status_code = 200
            elif result_1["title"] == 7:
                messege = {
                "amount" : result_1["title"],
                "result_code" : 247,
                "status" : 200,
                "total_pack_price" : total_pack_price,
                "num_of_ref" : num_of_ref,
                "progress" : number_of_cond
                }
                respone = jsonify(messege)
                respone.status_code = 200
        else:
            messege = {
                "amount" : 0,
                "result_code" : 248,
                "status" : 200
                }
            respone = jsonify(messege)
            respone.status_code = 200
    
    
    
    except Exception as e:
        conn.rollback()
        print(e)
        messege = {
                        "amount" : result_1["title"],
                        "result_code" : 243,
                        "status" : 500
                        }
        respone = jsonify(messege)
        respone.status_code = 200
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone

@application.route('/rateReturn', methods=['POST'])
async def rate_return():
    try:
        _json = request.json
        _pack_level = _json['pack_level']
        _expacted_date = _json["expacted_date"]
        _ref_num = _json["ref_num"]
        print("try 구문 진입")
        #페키지 래밸, 예치일, 추천수를 받아온다.
        if _pack_level <= 7 and _expacted_date <= 365 and _ref_num <= 10:
            print("input 값은 문제가 없습니다.")
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)	
            sqlQuery_0 = """SELECT * FROM tb_info_packages 
                            WHERE idx = %s;
                            """
            bindData_0 = (_pack_level)
            cursor.execute(sqlQuery_0,bindData_0)
            pack_data = cursor.fetchone()
            print("쿼리로 패키지 데이터를 받아 오는데 성공했습니다.")
            cursor.execute("SELECT usdt FROM tb_setting_exchange_rate")
            rm_price = cursor.fetchone()
            print("쿼리로 RM 가격을 받아오는데 성공했습니다 가격은 {}.".format(rm_price["usdt"]))
            #서버에서 패키지 정보를 가져온다.
            investment_money = pack_data["packages_price"]
            robot_num = pack_data["mobile_suite_cnt"]
            effective_inf_level = influnece_lv_counter(_ref_num,pack_data["influence_lv"])
            #실제 영향을 끼치는 패키지 래밸을 계산
            total_earned_money = 0
            
            total_asset: int = 0
            total_asset = total_asset + investment_money
            num_of_code_500 = 0
            expacted_date_earned = 0
            total_date_earned = 0
            addtional_invest = 0
            print("모든 변수선언에 성공했습니다.")
            #계산에 필요한 변수 선언
            print("for 문 진입")
            for i in range(365): #400일때 까지 계산
                if i <= _expacted_date: #입력받은 예치일까지 계산결과는 따로 저장
                    #print("expacteD_date if 구문 진입")
                    total_earned_money = total_earned_money + (robot_num*20*0.7 + _ref_num*0.6*robot_num*20*0.7 + num_of_code_500*0.6*20*0.7)*rm_price["usdt"]
                    if total_earned_money >= 500:
                        total_earned_money = total_earned_money - 500
                        num_of_code_500 = num_of_code_500 + 1
                        robot_num = robot_num + 1
                        addtional_invest = addtional_invest + 500
                    expacted_date_earned = addtional_invest + total_earned_money
                    print("{} 일째, 로봇 수는 총 {} 개이고, 하부 500 코드의 수는 {} 이고, 추천수는 {} 이다.".format(i+1,robot_num,num_of_code_500,_ref_num))
                    print("{} 일째, 총 수익은 {} 이다 ".format(i+1,expacted_date_earned))
                    print("{} 일째, 추가 투자금은 총 {} 이고, 총 수익은 {} 이다.".format(i+1, addtional_invest, total_earned_money))
                    expacted_date_robot = robot_num
                else:
                    #print("expacteD_date else 구문 진입")
                    total_earned_money = total_earned_money + (robot_num*20*0.7 + _ref_num * 0.6 * robot_num * 20  * 0.7 + num_of_code_500*0.6*20)*rm_price["usdt"]
                    if total_earned_money >= 500:
                        total_earned_money = total_earned_money - 500
                        num_of_code_500 = num_of_code_500 + 1
                        robot_num = robot_num + 1
                        addtional_invest = addtional_invest + 500
                    total_date_earned = expacted_date_earned+ addtional_invest + total_earned_money
            
            
            messege = {
                            "expacted_date_earned" : round(expacted_date_earned,3) ,
                            "total_date_earned" : round(total_date_earned,3),
                            "total_invest" : round(investment_money,3),
                            "expacted_date_robot" : round(expacted_date_robot),
                            "total_robot" : round(robot_num),
                            "result_code" : 260,
                            "status" : 200,
                            "graph_data" : [{ "y" : investment_money , "x" : 0  },
                                            { "y" : round(expacted_date_earned,3) , "x" : _expacted_date },
                                            {  "y" : round(total_date_earned,3), "x" : 365}]
                            
                            }
            respone = jsonify(messege)
            respone.status_code = 200


    except Exception as e:
        messege = {
                            "expacted_date_earned" : 0 ,
                            "total_date_earned" : 0,
                            "total_invest" :0,
                            "result_code" : 261,
                            "status" : 500
                            }
        respone = jsonify(messege)
        respone.status_code = 200

        #conn.rollback()
        print(e)
    finally:
        
        #cursor.close() 
        #conn.close()  
        return respone

@application.route('/getWallet_sol', methods=['POST'])
async def getWallet_sol():

    client = AsyncClient("https://api.mainnet-beta.solana.com")   
    account = Keypair().generate()
    
    await client.close()
    
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
        respone.status_code = 200
        return respone
    except Exception as e:
        massage = {
                'status' : 500,
                'result_code' : 231,
                'pubkey' : None,
                'seckey' : None
            }
        respone = jsonify(massage)
        respone.status_code = 200
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
            "result_code": 200 
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
        respone.status_code = 200
        print(e)
    
    finally:
        return respone
    
@application.route('/getWallet_bnb', methods=['POST'])
async def getWallet_bnb():

    try:
        priv = secrets.token_hex(32)
        priv = secrets.token_hex(32)
        private_key = "0x" + priv
        print ("SAVE BUT DO NOT SHARE THIS:", private_key)
        acct = Account.from_key(private_key)
        print("Address:", acct.address)
        _wallet_pubkey = acct.address
        _wallet_seckey = private_key
       
    
        massage = {
                'status' : 200,
                'result_code' : 230,
                'pubkey' : str(_wallet_pubkey),
                'seckey' : str(_wallet_seckey)
                }
        respone = jsonify(massage)
        respone.status_code = 200
        return respone
    except Exception as e:
        massage = {
                'status' : 500,
                'result_code' : 231,
                'pubkey' : None,
                'seckey' : None
            }
        respone = jsonify(massage)
        respone.status_code = 200
        #conn.rollback()
        print(e)
    finally:
        
        #cursor.close() 
        #conn.close()  
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
            respone.status_code = 200
        else:
            respone = jsonify({
                "amount" : 0,
                'status' : 200,
                'result_code' : 232

            })
            respone.status_code = 200
        
    except Exception as e:
        
        respone = jsonify({
            "amount" : 0,
            'status' : 500,
            'result_code' : 233

        })
        respone.status_code = 200
    
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
        respone.status_code = 200

        await client.close()
        return respone
        
    except Exception as e:
        massage = {
                'status' : 500,
                'result_code' : 233,
                "amount" : 0
                }
        respone = jsonify(massage)
        respone.status_code = 200
        #conn.rollback()
        print(e)
    finally:
        
        #cursor.close() 
        #conn.close()  
        return respone
        
@application.route('/next_package', methods=['POST'])
async def next_package():
    try:        
        _json = request.json
        _member_idx = _json['member_idx']
        _wanted_pack = _json["pack_level"]
        if _member_idx and request.method == 'POST':
            
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)		
            sqlQuery_0 = """SELECT * FROM (SELECT a.member_idx, a.title, b.refiner_lv, b.influence_lv FROM tb_org_chart AS a
                            JOIN tb_land AS b
                            ON a.member_idx = b.owner_idx
                            WHERE a.member_idx = %s) as x
                            JOIN tb_info_packages
                            ON x.refiner_lv = tb_info_packages.packages_level;
                            """
            sqlQuery_0_1 = """SELECT * FROM tb_info_packages 
                                WHERE packages_level = %s;
                            """
            sqlQuery_0_2 = """SELECT * FROM tb_member
                                WHERE idx = %s"""
                            #자신의 페키지 래밸을 가져온다.
                            #다음 페키지 래밸이 되면 인플루언스 래밸이 얼마가 되는지를 계산한다.
                            #멤버를 기입하면 해당 맴버가 받을 수 있는 예상 수익을 계산해준다. f(맴버, 패키지래밸)
                            #수익은 자기 채굴 수익, 하부 수익으로 나뉜다.
                            #자기 채굴 수익 = 로봇 대수 * RM 가격/달러 * 30 (매달)
                            #하부 수익은 대수로 카운트 한다. 
                            #쿼리로 자신의 인플루언스 래밸 이상, 다음 패키지의 인플루언스래밸 이하의 맴버들을과 그들의 로봇 갯수를 전부 가져온다(조인으로 가져오면될듯)
                            #각 패키지 별로 한달 예상 수익을 계산하고 그 결과를 대수에 반영하여 추가적으로 수당에의한 수익을 계산한다. 
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
                            WHERE lvl-1 <= %s ) a
                            JOIN tb_land b
                            ON a.member_idx = b.owner_idx
                            ORDER BY a.member_idx DESC) c 
                            JOIN tb_info_packages d
                            on c.refiner_lv = d.refiner_type
                            ;"""
                            
           
            bindData_0 = (_member_idx)
            cursor.execute(sqlQuery_0, bindData_0)
            result_1 = cursor.fetchone()
            bindData_0_1 = (_wanted_pack)
            cursor.execute(sqlQuery_0_1,bindData_0_1)
            result_1_1 = cursor.fetchone()
            cursor.execute("SELECT usdt FROM tb_setting_exchange_rate")
            rm_price = (cursor.fetchone())["usdt"]
            if result_1 :
                cursor.execute(sqlQuery_0_2,bindData_0)
                member_info = cursor.fetchone()

                ref_count = member_info["referral_regular_count"]
                user_title = result_1["title"]
                
                
            
                price_per_month = 0
                price_per_month_by_mining = 0
                
                
                next_lv = result_1_1["influence_lv"] #페키지의 인플루언스 래밸
                true_inf_lv = influnece_lv_counter(ref_count, next_lv)
                bindData_1 = (_member_idx,true_inf_lv)
                cursor.execute(sqlQuery_1,bindData_1)
                
                next_pack = cursor.fetchall() #현재 자신의 영향력 안에 있는 유저를 전부 모은다.. 
                
                if next_pack:
                    print("현재의 influence lv 는 ", true_inf_lv)
                    
                    total_robot = int(result_1_1["mobile_suite_cnt"])
                    price_per_month_by_mining = int(total_robot) * float(rm_price) * 30 * 20 * 0.7
                    print("마이닝에 의한 수당은 ", price_per_month_by_mining)
                    print("해당 인플루언스 래밸일때, 나는 {} 까지 수당을 받을 수 있다.".format(len(next_pack)))
                    for row in next_pack:
                        print("for 구문 진입")
                        row_income = row["mobile_suite_cnt"] * rm_price * 30 * 20 * 0.7
                        
                        if row["title"] > user_title:
                            row_income = row_income * 0.5
                        elif row["title"] <= user_title:
                            row_income = row_income * 1

                        if row["lvl"]-1 == 0:
                            row_income = row_income*0
                        elif row["lvl"]-1 == 1:
                            row_income = row_income*0.5
                        elif row["lvl"]-1 == 2:
                            row_income = row_income*0.30
                        elif row["lvl"]-1 == 3:
                            row_income = row_income*0.25
                        elif row["lvl"]-1 == 4:
                            row_income = row_income*0.15
                        elif row["lvl"]-1 == 5:
                            row_income = row_income*0.10
                        elif row["lvl"]-1 == 6:
                            row_income = row_income*0.07
                        elif row["lvl"]-1 == 7:
                            row_income = row_income*0.06
                        elif row["lvl"]-1 == 8:
                            row_income = row_income*0.05
                        elif row["lvl"]-1 == 9:
                            row_income = row_income*0.04
                        elif row["lvl"]-1 == 10:
                            row_income = row_income*0.03
                        elif row["lvl"]-1 == 11:
                            row_income = row_income*0.03
                        elif row["lvl"]-1 == 12:
                            row_income = row_income*0.02
                        elif 13<= row["lvl"]-1 <= 15:
                            row_income = row_income*0.01
                        elif 16<= row["lvl"]-1 <= 20:
                            row_income = row_income*0.005
                        print("추가 row_income 값", row_income)
                        price_per_month = price_per_month + row_income
                        #해당 row 유저에 의해서 발생할 예상 수익이 계산된다.
                    print("라인에 의해 발생하는 수당은 ",price_per_month)
                    total_add_income = round(price_per_month + price_per_month_by_mining,3)
                    messege = {
                            "amount" : total_add_income,
                            "line_amount" : round(price_per_month,3),
                            "pack_amount" : round(price_per_month_by_mining,3),
                            "result_code" : 250,
                            "status" : 200
                            }
                    respone = jsonify(messege)
                    respone.status_code = 200
                else:
                    price_per_month = 0
                    price_per_month_by_mining = result_1_1["mobile_suite_cnt"] * float(rm_price) * 30 * 20 * 0.7
                    total_add_income = round(price_per_month + price_per_month_by_mining,3)
                    messege = {
                            "amount" : total_add_income,
                            "line_amount" : round(price_per_month,3),
                            "pack_amount" : round(price_per_month_by_mining,3),
                            "result_code" : 250,
                            "status" : 200
                            }
                    respone = jsonify(messege)
                    respone.status_code = 200
            else:
                price_per_month = 0
                price_per_month_by_mining = result_1_1["mobile_suite_cnt"] * float(rm_price) * 30 * 20 * 0.7
                total_add_income = round(price_per_month + price_per_month_by_mining,3)
                messege = {
                            "amount" : total_add_income,
                            "line_amount" : round(price_per_month,3),
                            "pack_amount" : round(price_per_month_by_mining,3),
                            "result_code" : 250,
                            "status" : 200
                            }
                respone = jsonify(messege)
                respone.status_code = 200
        else:
            messege = {
                    "amount" : 0,
                    "line_amount" : 0,
                    "pack_amount" : 0,
                    "result_code" : 251,
                    "status" : 200
                    }
            respone = jsonify(messege)
            respone.status_code = 200
    
    
    except Exception as e:
        conn.rollback()
        print(e)
        messege = {
                        "amount" : 0,
                        "line_amount" : 0,
                        "pack_amount" : 0,
                        "result_code" : 252,
                        "status" : 500
                        }
        respone = jsonify(messege)
        respone.status_code = 200
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone


@application.route('/settlement', methods=['POST'])
async def settlement():
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
            sqlQuery_1 = """SELECT * FROM (SELECT * FROM (WITH RECURSIVE cte (member_idx, member_id, parent_idx , title , lvl) AS (
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
                            LIMIT 21) a
                            JOIN tb_land b
                            ON a.member_idx = b.owner_idx
                            ) AS k
                            INNER JOIN tb_member AS j
                            ON k.member_idx = j.idx
                            ORDER BY k.member_idx DESC;"""
            bindData_0 = (_member_idx)
            bindData_1 = (_member_idx)
            
            cursor.execute(sqlQuery_0,bindData_0)
            user_info = cursor.fetchone()
            #Rows = 유저 정보를 받아온다.
            cursor.execute(sqlQuery_1,bindData_1)
            Rows = cursor.fetchall()
            #Rows = 해당 맴버 상위로 20개 를 가져온다.(최대, 맴버 테이블도 같이 조인한다.)
            result_list_1 = []
            result_list_2 = []
            result_list_3 = [] #
            if Rows and user_info:
                if Rows[0]["lvl"] == 1:
                    sqlQuery_s = """select nextval(sq_group_stl)"""
                    cursor.execute(sqlQuery_s)
                    #인덱스 올려준다.
                    group_idx = cursor.fetchone()["nextval(sq_group_stl)"]
                    print("group idx is ", group_idx)
                    _mineral_start = user_info["mineral_amount"] 
                    _mineral_amount = user_info["mineral_amount"]*0.7
                    _fee_amount = user_info["mineral_amount"]*0.7
                    _title = user_info["title"]
                    _ref_enery = user_info["refining_energy"]
                
                    if _ref_enery >= _mineral_amount:
                        effective_mineral = _mineral_amount
                    else:
                        effective_mineral = _ref_enery
                    if effective_mineral != 0:
                    # 미네랄에서 새금을 제외한다. 만약 남은 re 보다 많다면 그대로 정산되고, re가 부족하다면 남은 re 만큼 정산된다.
                        for r in Rows:
                            if influnece_lv_counter(r["referral_regular_count"],r["influence_lv"]) >= r["lvl"]-1: #해당 로우의 유저의 인플루언스 래밸(수당래밸) 이 발생자와의 거리보다 크거나 같아야지 수당을 받을수있다.
                                if r["title"] >= (_title): #해당 로우의 유저의 타이틀이 발생자보다 같거나 높으면 정가로, 아니면 반값
                                    refined_mineral = effective_mineral
                                else:
                                    refined_mineral = effective_mineral * 0.5
                                
                                if r["lvl"]-1 == 0:
                                    refined_mineral = refined_mineral
                                elif r["lvl"]-1 == 1:
                                    refined_mineral = refined_mineral*0.6
                                elif r["lvl"]-1 == 2:
                                    refined_mineral = refined_mineral*0.35
                                elif r["lvl"]-1 == 3:
                                    refined_mineral = refined_mineral*0.25
                                elif r["lvl"]-1 == 4:
                                    refined_mineral = refined_mineral*0.15
                                elif r["lvl"]-1 == 5:
                                    refined_mineral = refined_mineral*0.10
                                elif r["lvl"]-1 == 6:
                                    refined_mineral = refined_mineral*0.07
                                elif r["lvl"]-1 == 7:
                                    refined_mineral = refined_mineral*0.06
                                elif r["lvl"]-1 == 8:
                                    refined_mineral = refined_mineral*0.05
                                elif r["lvl"]-1 == 9:
                                    refined_mineral = refined_mineral*0.04
                                elif r["lvl"]-1 == 10:
                                    refined_mineral = refined_mineral*0.03
                                elif r["lvl"]-1 == 11:
                                    refined_mineral = refined_mineral*0.03
                                elif r["lvl"]-1 == 12:
                                    refined_mineral = refined_mineral*0.02
                                elif 13<= r["lvl"]-1 <= 15:
                                    refined_mineral = refined_mineral*0.01
                                elif 16<= r["lvl"]-1 <= 20:
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
                        respone.status_code = 200 
                    else:
                        message = {
                        "status" : 200,
                        "amount" : 0,
                        "result_code" : 221
                        }
                        respone = jsonify(message)
                        respone.status_code = 200
                else:
                    message = {
                        "status" : 200,
                        "amount" : 0,
                        "result_code" : 222
                        }
                    respone = jsonify(message)
                    respone.status_code = 200
            else:
                message = {
                    "status" : 200,
                    "amount" : 0,
                    "result_code" : 223
                }
                respone = jsonify(message)
                respone.status_code = 200
    except Exception as e:
        conn.rollback()
        message = {
                    "status" : 500,
                    "amount" : 0,
                    "result_code" : 224
        }
        respone = jsonify(message)
        respone.status_code = 200
        print(e)
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone

@application.route('/end_mining_all', methods=['POST'])
async def total_end_mining():
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
                respone.status_code = 200
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
                respone.status_code = 200             
        else:
            message = {
                "status" : 200,
                "amount" : 0,
                "result_code" : 211
            }
            respone = jsonify(message)
            respone.status_code = 200
    except Exception as e:
        conn.rollback()
        message = {
                "status" : 500,
                "amount" : 0,
                "result_code" : 212
            }
        respone = jsonify(message)
        respone.status_code = 200
        print(e)
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone

@application.route('/start_mining_all', methods=['POST'])
async def total_start_mining():
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
            respone.status_code = 200
        else:
            message = {
                "status" : 200,
                "amount" : 0,
                "result_code" : 214
            }
            respone = jsonify(message)
            respone.status_code = 200
    except Exception as e:
        conn.rollback()
        message = {
                "status" : 500,
                "amount" : 0,
                "result_code" : 215
            }
        respone =jsonify(message)
        respone.status_code = 200
        print(e)
    finally:
        conn.commit()
        cursor.close() 
        conn.close()  
        return respone


@application.route('/end_mining', methods=['POST'])
async def end_mining():
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
async def start_mining():
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