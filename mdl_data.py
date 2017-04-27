# encoding = utf-8
# name = mdl_date.py
# Author: Aaron Yang
# 2016.07.03
import MySQLdb
import mdl_db_manager


class MdlData(object):

    def __init__(self):
        self.db_connection = mdl_db_manager.MdlDatabase()
        self.ls_course_id = []              # store the all the id for the course which contains block learning_analysis
        self.ls_log_tmp = []                # store the tmp log info we need
        self.ls_log = []                    # store the log info
        self.ls_course_elo = []             # store how many elos does a course have, e.g: elo 1,2,3...
        self.ls_elo_component = []          # store the component for each single elo
        self.ls_student_tmp = []            # store student information for every calculation
        self.ls_student = []                # store the calculated result
        self.ls_forum_id = []               # store the forum id result
        self.ls_forum_component_id = []
        self.ls_ouwiki_component_id = []
        self.ls_posts = []

        self.ls_sources = []

    def is_connected(self):
        return self.db_connection.is_connected()

    def get_sources(self):
        self.ls_sources = self.db_connection.fetch_data("select a.id,a.sources,a.coder,b.sources,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id "
                                                               "AND a.coder = \"c2\" and b.coder=\"c1\" "
                                                               " union "
                                                               "select a.id,a.sources,a.coder,b.sources,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id AND "
                                                               "a.coder = \"c1\" and b.coder=\"c2\" ", )
        return self.ls_sources

    # Get course id from moodle system table according to the context
    def get_course_id(self):
        self.ls_course_id = self.db_connection.fetch_data("SELECT `instanceid` AS courseid FROM `mdl_context` WHERE `id` IN"
                                               " (SELECT `parentcontextid` FROM `mdl_block_instances` "
                                               "WHERE `blockname` = %s)", "learning_analysis")
        return self.ls_course_id

    def get_course_id_grade_prediction(self):
        self.ls_course_id = self.db_connection.fetch_data("SELECT `instanceid` AS courseid FROM `mdl_context` WHERE `id` IN "
                                                          " (SELECT `parentcontextid` FROM `mdl_block_instances` "
                                                          "WHERE `blockname` = %s)", "grade_prediction")
        return self.ls_course_id

    def get_course_elo(self, course_id):
        self.ls_course_elo = self.db_connection.fetch_data("SELECT DISTINCT(`elo`) AS elo FROM mdl_elo_component WHERE `course` = %s;", course_id)
        return self.ls_course_elo

    def get_course_elo_component(self, course_id):
        self.ls_elo_component = self.db_connection.fetch_data("SELECT * FROM mdl_elo_component WHERE `course` = %s", course_id)
        return self.ls_elo_component

    def get_log(self, course_id, component_id, user_id):
        list_par = [str(course_id), str(component_id), str(user_id)]
        self.ls_log = self.db_connection.fetch_data("SELECT `id` AS pk, `component`, `courseid`,`userid`, `contextinstanceid` AS elo_component_id, "
                                                    "`action`, `target`, from_unixtime(`timecreated`) AS create_time "
                                                    "FROM `mdl_logstore_standard_log`"
                                                    "WHERE `courseid` = %s AND `contextinstanceid` = %s"
                                                    "AND `userid` = %s AND `component` != 'core' ORDER BY "
                                                    "`id` ASC", list_par)
        return self.ls_log

    # role id = 5 means student
    def get_student(self, course_id):
        self.ls_student = self.db_connection.fetch_data("SELECT `mdl_user_enrolments`.`userid`, "
                                                        "CONCAT(`mdl_user`.`firstname`,' ',`mdl_user`.`lastname`) "
                                                        "AS `user_name` FROM `mdl_user_enrolments`, `mdl_user` "
                                                        "WHERE `mdl_user_enrolments`.`enrolid` IN(SELECT `id` FROM "
                                                        "`mdl_enrol` WHERE `courseid` = %s) AND `mdl_user_enrolments`.`userid` IN"
                                                        "(SELECT `userid` FROM `mdl_role_assignments` WHERE "
                                                        "`roleid` = 5) AND `mdl_user_enrolments`.`userid` = `mdl_user`.`id`", course_id)     #role id = 5 <=> student
        return self.ls_student

    def get_user_by_user_id(self, user_id):
        return self.db_connection.fetch_data("SELECT `id`, `username`, CONCAT(`firstname`, ' ', `lastname`) AS `fullname`, `email` FROM `mdl_user` WHERE `id` = %s", user_id)

    def get_role_by_user_id(self, user_id):
        return self.db_connection.fetch_data("SELECT `id`,`roleid`,`contextid`, `userid` FROM `mdl_role_assignments` WHERE `userid` = %s", user_id)

    def update(self, target, params=[]):
        if target == "ue_mapping":
            self.db_connection.clear_data("TRUNCATE TABLE `mdl_elo_ue_mapping`")
            sql = "INSERT INTO `mdl_elo_ue_mapping` (`user_id`, `user_name`, `course_id`, `elo_id`, `engagement`) VALUES (%s, %s, %s, %s, %s) "
            return self.db_connection.insert_data(sql, params)

        elif target == "uc_mapping":
            self.db_connection.clear_data("TRUNCATE TABLE `mdl_elo_uc_mapping`")
            sql = "INSERT INTO `mdl_elo_uc_mapping` (`user_id`, `user_name`, `course_id`, `elo_id`, `component_id`, `component_name`, `engagement`) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            return self.db_connection.insert_data(sql, params)

        # update norm weight
        elif target == "ec_mapping":
            self.db_connection.clear_data("TRUNCATE TABLE `mdl_elo_ec_mapping`")
            sql = "INSERT INTO `mdl_elo_ec_mapping` (`course_id`,`elo_id`,`component_id`,`component_name`,`avg_engagement`) VALUES (%s, %s, %s, %s, %s) "
            return self.db_connection.insert_data(sql, params)

        elif target == "elo_component":
            sql = "UPDATE `mdl_elo_component` SET `weight` = %s WHERE `id` = %s"
            return self.db_connection.update_data(sql, params)
        elif target == "forum_post_relation":
            self.db_connection.clear_data("TRUNCATE TABLE `mdl_elo_forum_post_relation`")
            sql = "INSERT INTO `mdl_elo_forum_post_relation` (`post_id`,`user_from_id`,`user_from_name`,`user_to_id`,`user_to_name`,`forum_id`,`forum_component_id`) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            return self.db_connection.insert_data(sql, params)
        elif target == "unique_relation":
            self.db_connection.clear_data("DELETE FROM `mdl_elo_forum_relation_stats`")
            sql = "INSERT IGNORE INTO `mdl_elo_forum_relation_stats` (`user_from_id`,`user_from_name`,`user_to_id`,`user_to_name`,`forum_component_id`, `total_post_count`) VALUES(%s, %s, %s, %s, %s, %s)"
            return self.db_connection.insert_data(sql, params)
        elif target == "relation_stats_sql":
            sql = "UPDATE mdl_elo_forum_relation_stats AS a SET a.total_post_count = (SELECT COUNT(*) FROM mdl_elo_forum_post_relation AS b WHERE a.user_from_id = b.user_from_id AND a.user_to_id = b.user_to_id AND a.forum_component_id = b.forum_component_id)"
            return self.db_connection.update_data(sql)
        elif target == "relation_stats":
            sql = "UPDATE mdl_elo_forum_relation_stats AS a SET a.total_post_count = %s WHERE a.user_from_id = %s AND a.user_to_id = %s AND a.forum_component_id = %s"
            return self.db_connection.update_data(sql, params)
        elif target == "score_prediction":
            #print params
            sql = "REPLACE INTO mdl_sp_result (userid, courseid, cnt_viewcoursesection,cnt_viewforums,cnt_viewurls,cnt_submitassign," \
                  "cnt_quizclose,cnt_quizstart,cnt_quizcontinue,cnt_wikiedit,cnt_viewsubmitassign,predicted_score) " \
                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            return self.db_connection.insert_data(sql, params)
        else:
            sql = "UPDATE `mdl_elo_info` SET `avg_engagement` = %s WHERE `course` = %s AND elo = %s"
            return self.db_connection.update_data(sql, params)

    # Added 2016.10
    def get_forum_id(self, course_id):
        self.ls_forum_id = self.db_connection.fetch_data("SELECT id AS forum_id FROM mdl_forum WHERE `course` = %s ORDER BY id", course_id)
        return self.ls_forum_id

    # module = 9 means forum
    # EDU MOODLE module = 7 means forum
    # So do not hard code but select from table
    def get_forum_component_id(self, course_id):
        self.ls_forum_component_id = self.db_connection.fetch_data("SELECT id AS forum_component_id FROM mdl_course_modules WHERE `course` = %s AND `module` = (select id from mdl_modules where name = 'forum') ORDER BY id", course_id)
        return self.ls_forum_component_id

    def get_forum_posts(self, forum_id):
        list_par = [str(forum_id)]
        return self.db_connection.fetch_data("SELECT b.id, b.parent, b.userid FROM mdl_forum_posts AS b WHERE b.discussion IN(SELECT id FROM mdl_forum_discussions AS a WHERE a.forum = %s) ORDER BY b.id", list_par)

    # Added 2016.10.23
    def get_all_post_relation(self):
        ls_post_relation = self.db_connection.fetch_data("SELECT user_from_id, user_to_id, forum_component_id FROM mdl_elo_forum_post_relation")
        return ls_post_relation

    def get_ouwiki_component_id(self, course_id):
        self.ls_ouwiki_component_id = self.db_connection.fetch_data("SELECT id AS ouwiki_component_id FROM mdl_course_modules WHERE `course` = %s AND `module` = (select id from mdl_modules where name = 'ouwiki') ORDER BY id", course_id)
        return self.ls_ouwiki_component_id

    # Tmp test use
    def beta_get_course_id(self):
        return self.db_connection.fetch_data("SELECT DISTINCT(course) AS courseid FROM mdl_forum")

    def beta_get_student(self):
        return self.db_connection.fetch_data("SELECT b.id AS userid, CONCAT(b.firstname, ' ', b.lastname) AS user_name FROM mdl_user AS b WHERE b.id IN (SELECT DISTINCT(userid) FROM mdl_forum_posts)")

    def beta_get_posts(self):
        return self.db_connection.fetch_data("SELECT b.id, b.parent, b.userid FROM mdl_forum_posts AS b ORDER BY b.id")

    def close_connection(self):
        self.db_connection.close_conn()

    # Added 2017.03
    # Functions below are for score prediction
    def sp_get_view_course_section(self, course_id):
        list_par = [str(course_id)]
        return self.db_connection.fetch_data("SELECT userid, count(*) AS cnt FROM mdl_logstore_standard_log WHERE courseid = %s and action = 'viewed' GROUP BY userid", list_par)

    def sp_get_view_forum(self, course_id):
        list_par = [str(course_id), "mod_forum"]
        return self.db_connection.fetch_data("SELECT userid, count(*) AS cnt FROM mdl_logstore_standard_log WHERE courseid = %s component = %s and action = 'viewed' GROUP BY userid", list_par)

    def sp_get_log_stats(self, course_id, action, component="", target=""):
        if component is "":
            list_par = [str(course_id) ,str(action)]
            return self.db_connection.fetch_data("SELECT `userid`, count(*) AS `cnt` FROM `mdl_logstore_standard_log` \
                                                    WHERE `courseid` = %s AND `action` = %s GROUP BY `userid`", list_par)
        else:
            if target is "":
                if len(action) == 1:
                    list_par = [str(course_id), str(component), str(action[0])]
                    return self.db_connection.fetch_data("SELECT `userid`, count(*) AS `cnt` FROM `mdl_logstore_standard_log` \
                                                            WHERE `courseid` = %s AND `component`= %s AND `action` = %s GROUP BY `userid`", list_par)
                if len(action) == 2:
                    list_par = [str(course_id), str(component), str(action[0]), str(action[1])]
                    return self.db_connection.fetch_data("SELECT `userid`, count(*) AS `cnt` FROM `mdl_logstore_standard_log` \
                                                            WHERE `courseid` = %s AND `component`= %s AND `action` = %s OR `action` = %s GROUP BY `userid`", list_par)
            else:
                list_par = [str(course_id), str(component), str(action), str(target)]
                return self.db_connection.fetch_data("SELECT `userid`, count(*) AS `cnt` FROM `mdl_logstore_standard_log` \
                                                            WHERE `courseid` = %s AND `component`= %s AND `action` = %s AND `target` = %s GROUP BY `userid`", list_par)





