import logging
import os
import oracledb
import csv
import configparser

class Db_Service:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('data_migration_tool')
        self.logger.info("init class User_Merge.")
        self.db_init()
        self.root_uid = None
        self.root_name = None

        self.root_approve_uid = None
        self.root_approve_name = None
    
    def db_init(self):
        self.logger.info("enter into db_init.")
        try:
            config = configparser.ConfigParser()
            config.read("./data/config.ini")
            if not config:
                self.logger.error("project config data not found.")
                return
        
            self.user = config.get('Database', 'user')
            self.password = config.get('Database', 'password')
            self.dsn = config.get('Database', 'dsn')
            if (not self.user) or (not self.password) or (not self.dsn):
                self.logger.error(f"oracle db config is not correct.")
                return
        except Exception as e:
            self.logger.info(f"An error occurred: {e}")
        self.logger.info("end db_init.")

    def get_root_info(self):
        self.ini_db_conncetion(self.user, self.password, self.dsn)
        if not self.connection:
            return None
        self.query_root_uid()
        if not self.root_uid:
            return None
        res = self.query_obj_prop_uid(self.root_uid)
        res_approve = self.query_obj_prop_uid(self.root_approve_uid)
        self.close_db_conncetion()
        return res, res_approve

    def query_object_info(self, uid):
        self.ini_db_conncetion(self.user, self.password, self.dsn)
        if not self.connection:
            return None, None
        if not uid:
            return None
        res = self.query_obj_prop_uid(uid)
        self.close_db_conncetion()
        return res

    def query_root_uid(self):
        self.logger.info("enter inot func query_root_uid.")
        try:
            cursor = self.connection.cursor()
            sql = """
                select /*+ PARALLEL(CLASS_HIERARCHY, 4) */  cid as "uid", a.name as object_name
                from class_info a where a.name = 'Repository' or a.name = 'Approve' 
            """
            cursor.execute(sql)
            for row in cursor.fetchall():
                if row[1] == 'Repository':
                    self.root_uid = row[0]
                    self.root_name = row[1]
                if row[1] == 'Approve':
                    self.root_approve_uid = row[0]
                    self.root_approve_name = row[1]

        except Exception as e:
            self.logger.info(f"An error occurred: {e}")
        finally:
            if cursor:
                cursor.close()
        self.logger.info("end func query_root_uid.")

    def query_obj_prop_uid(self, uid):
        self.logger.info("enter into func query_obj_prop_uid.")
        
        cursor = self.connection.cursor()
        obj_prop = {}

        sql = """
                select /*+ PARALLEL(CLASS_HIERARCHY, 4) */  cid as cid, a.name as object_name, a.type_id as object_type, a.revision as item_revision_id,
                a.item_id as item_id, a.remark as remarks,
                TO_CHAR(a.lock_status) as locked, a.parent_object_id as upper_object_uid, 
                TO_CHAR(a.create_date, 'YYYY/MM/DD HH24:MI:SS') as creation_date,
                TO_CHAR(a.last_update_date, 'YYYY/MM/DD HH24:MI:SS') as last_mod_date,
                LOWER(a.owner_user_id) as owning_user,LOWER(a.last_update_user_id) as last_mod_user,
                a.template_folder as notice_mail_tmpl_folder
                from class_info a
                """
        class_info = self.exec_dbsql_with_uid(cursor, sql, uid)
        obj_prop['class_info'] = class_info

        sql = """
                select cid as cid, addon_operation_flg1 as addon_operation_flg1, addon_operation_flg2 as addon_operation_flg2, 
                addon_operation_flg3 as addon_operation_flg3,  addon_operation_flg4 as addon_operation_flg4,  addon_operation_flg5 as addon_operation_flg5,   
                addon_operation_flg6 as addon_operation_flg6,  addon_operation_flg7 as addon_operation_flg7,  addon_operation_flg8 as addon_operation_flg8,    
                addon_operation_flg9 as addon_operation_flg9,  addon_operation_flg10 as addon_operation_flg10,  addon_operation_flg11 as addon_operation_flg11,  
                addon_operation_flg12 as addon_operation_flg12,  addon_operation_flg13 as addon_operation_flg13,  addon_operation_flg14 as addon_operation_flg14,   
                addon_operation_flg15 as addon_operation_flg15,  addon_operation_flg16 as addon_operation_flg16,  addon_operation_flg17 as addon_operation_flg17,   
                addon_operation_flg18 as addon_operation_flg18,  addon_operation_flg19 as addon_operation_flg19,  addon_operation_flg20 as addon_operation_flg20, 
                addon_operation_flg21 as addon_operation_flg21
                from ADDON_OPERATION_FLG
                """
        
        addon_info = self.exec_dbsql_with_uid(cursor, sql, uid)
        obj_prop['addon_info'] = addon_info

        sql = """
                select cid as cid, reserve_phase as reserve_phaseF, TO_CHAR(update_count) as update_countF, reserve_status as reserve_statusF, specification_summary as specification_summaryF,
                    reserve_vehicle as reserve_vehicleF, ctrlDesign_id_dup as ctrlDesign_id_dupF, module_id_save as module_id_saveF,
                    module_id_dup as module_id_dupF, classification_id as classification_idF, layer_id_save as layer_id_saveF,
                    TO_CHAR(save_module_option) as save_module_optionF, TO_CHAR(module_recode_date, 'YYYY/MM/DD HH24:MI:SS') as module_recode_dateF, first_published_id as "1stpublished_idF",
                    note_save as note_saveF, targetModule_id_dup as targetModule_id_dupF, layer_id_dup as layer_id_dupF,
                    duplication_module_id as duplication_module_idF, abolished_module_id as abolished_module_idF, domain_name as domain_name,
                    domain_code as domain_code, follow_setting_cancel as follow_setting_cancel, product_type as product_type,
                    TO_CHAR(register_evidencedata) as register_evidencedata, deliverable_id_dup as deliverable_id_dup, TO_CHAR(duplicate_date, 'YYYY/MM/DD HH24:MI:SS') as duplicate_date,
                    vehicle_and_engine_code as vehicle_and_engine_code, phase_name as phase_name
                from SPEC_HISTORY
                """
        spec_history_info = self.exec_dbsql_with_uid(cursor, sql, uid)
        obj_prop['spec_history_info'] = spec_history_info


        sql = """
                select cid as cid, TO_CHAR(registration_deadline, 'YYYY/MM/DD HH24:MI:SS') as registration_deadline, vehicle_model as vehicle_model, engine_model as engine_model_web, transmission_model as transmission_model,
                    model_year as model_year, 
                    task_name as task_name, TO_CHAR(iquavis_connect) as iquavis_connect, iquavis_url as iquavis_url
                from IQUAVIS_CONNECT_INFO
                """
        iquavis_info = self.exec_dbsql_with_uid(cursor, sql, uid)
        obj_prop['iquavis_info'] = iquavis_info

        sql = """
                select cid as cid, TO_CHAR(follow_registered_flag) as follow_registered_flagF, LOWER(follow_registered_user) as follow_registered_userF
                from FOLLOW_SETTING_REGIST_STATUS
                """
        follow_status = self.exec_dbsql_with_uid(cursor, sql, uid)
        obj_prop['follow_status'] = follow_status

        sql = """
                select cid as cid, volume_data_name as file_name
                from VOLUME_DATA
                """
        file_info = self.exec_dbsql_with_uid(cursor, sql, uid)
        obj_prop['file_info'] = file_info

        sql = """
                select id as id, cid as cid, approval_no as approval_noF, approval_status as approval_statusF,approval_target_items as approval_target_itemsF,
                    publish_sub_folder as publish_sub_folderF, item_id_chg as module_id_chgF,
                    item_id_dup as module_id_dupF, registration_url as registration_urlF, LOWER(approval_requester) as approval_requesterF,
                    TO_CHAR(display_in_notice_list) as display_in_notice_listF, TO_CHAR(approval_date, 'YYYY/MM/DD HH24:MI:SS') as approval_dateF
                from APPROVAL_INFO
                """
        approval_info = self.exec_dbsql_list_with_uid(cursor, sql, uid)
        obj_prop['approval_info'] = approval_info

        sql = """
                select id as id, cid as cid, deliverables_type as deliverables_typeF, specification_no as specification_noF,user_name as division_user_nameF,
                    user_mail_address as user_mail_addressF, follower_name as division_follower_nameF,
                    follower_mail_address as follower_mail_addressF, task_url as task_urlF, TO_CHAR(delete_flag) as delete_flagF,
                    TO_CHAR(released_flag) as released_flagF, TO_CHAR(registered_flag) as registered_flagF, TO_CHAR(registered_date, 'YYYY/MM/DD HH24:MI:SS') as registered_dateF,
                    TO_CHAR(released_update_date, 'YYYY/MM/DD HH24:MI:SS') as released_update_date
                from FOLLOW_SETTING
                """
        follow_info = self.exec_dbsql_list_with_uid(cursor, sql, uid)
        obj_prop['follow_info'] = follow_info

        sql = """
                select id as id, cid as cid, LOWER(notice_mail_dest) as notice_mail_destF
                from NOTICE_INFO_RECEIVE
                """
        notice_receive_info = self.exec_dbsql_list_with_uid(cursor, sql, uid)
        obj_prop['notice_receive_info'] = notice_receive_info

        sql = """
                select id as id, cid as cid, LOWER(notice_mail_refusal) as refused_to_noticeF
                from NOTICE_INFO_REFUSAL
                """
        notice_refusal_info = self.exec_dbsql_list_with_uid(cursor, sql, uid)
        obj_prop['notice_refusal_info'] = notice_refusal_info
        
        self.logger.info("end func query_obj_prop_uid.")
        return obj_prop

    def query_children_uid_set(self, uid):
        self.logger.info("enter into func query_children_uid_set.")
        self.ini_db_conncetion(self.user, self.password, self.dsn)
        if not self.connection:
            return None
        children_uid_set = set() 
        sql = f"SELECT /*+ PARALLEL(CLASS_HIERARCHY, 4) */ CID FROM CLASS_HIERARCHY WHERE PARENT_CID = '{uid}'"
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                children_uid_set.add(row[0])
            cursor.close()
        except Exception as e:
            self.logger.error(f"An error occurred: {e}, sql:{sql}")
        
        self.logger.info("end func query_children_uid_set.")
        return children_uid_set
    
    def query_uid_and_name(self, children_uid_set):
        self.logger.info("enter into func query_uid_and_name.")
        uids = list(children_uid_set)
        uid_name_infos = {}
        try:
            cursor = self.connection.cursor()
            # one batch: 1000
            batch_size = 1000
            cnt = 0
            for i in range(0, len(uids), batch_size):
                cnt = cnt + 1
                batch_data = uids[i:i + batch_size]
                placeholders = ', '.join(f"'{str(item)}'" for item in batch_data)
                sql = f"SELECT /*+ PARALLEL(class_info, 4) */ a.cid,  a.name as object_name FROM class_info a WHERE a.cid IN ({placeholders})"
                cursor.execute(sql)
                rows = cursor.fetchall()
                for row in rows:
                    uid_name_infos[row[0]] = row[1]
            cursor.close()
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

        self.logger.info("end func query_uid_and_name.")
        return uid_name_infos
        

    
    def exec_dbsql_with_uid(self, cursor, sql, uid):
        map_info = None
        try:
            sql_filter = sql + f"  WHERE CID = '{uid}'"
            cursor.execute(sql_filter)
            columns = [column[0].lower() for column in cursor.description]
            for row in cursor.fetchall():
                map_info = {
                                key: (value if value is not None else '')  # 将 None 替换为 ''
                                for key, value in zip(columns[0:], row[0:])
                        }
        except Exception as e:
            self.logger.info(f"An error occurred: {e}, sql:{sql_filter}")
        return map_info
    
    def exec_dbsql_list_with_uid(self, cursor, sql, uid):
        set_info = []
        try:
            sql_filter = sql + f"  WHERE CID = '{uid}'"
            cursor.execute(sql_filter)
            columns = [column[0].lower() for column in cursor.description]
            rows = cursor.fetchall()
            for row in rows:
                row_dict = {key: (value if value is not None else '') for key, value in zip(columns, row)}
                set_info.append(row_dict)
        except Exception as e:
            self.logger.info(f"An error occurred: {e}, sql:{sql_filter}")
        return set_info     

    def ini_db_conncetion(self, user, password, dsn):
        try:
            self.connection = oracledb.connect(user=user, password=password, dsn=dsn)
        except Exception as e:
            self.logger.error(f"init oracle connection failed. error:{e}")

    def close_db_conncetion(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            self.logger.error(f"close oracle connection failed. error:{e}")






    
        







    