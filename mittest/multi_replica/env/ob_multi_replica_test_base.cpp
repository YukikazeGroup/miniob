/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <fstream>
#include "ob_multi_replica_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_defer.h"
#include "logservice/palf/election/utils/election_common_define.h"

namespace oceanbase
{
namespace unittest
{

int set_trace_id(char *buf) { return ObCurTraceId::get_trace_id()->set(buf); }

void init_log_and_gtest(int argc, char **argv)
{
  if (argc < 1) {
    abort();
  }

  std::string app_name = argv[0];
  app_name = app_name.substr(app_name.find_last_of("/\\") + 1);
  std::string app_log_name = app_name + ".log";
  std::string app_rs_log_name = app_name + "_rs.log";
  std::string app_ele_log_name = app_name + "_election.log";
  std::string app_gtest_log_name = app_name + "_gtest.log";
  std::string app_trace_log_name = app_name + "_trace.log";

  system(("rm -rf " + app_log_name + "*").c_str());
  system(("rm -rf " + app_rs_log_name + "*").c_str());
  system(("rm -rf " + app_ele_log_name + "*").c_str());
  system(("rm -rf " + app_gtest_log_name + "*").c_str());
  system(("rm -rf " + app_trace_log_name + "*").c_str());
  system(("rm -rf " + app_name + "_*").c_str());

  init_gtest_output(app_gtest_log_name);
  OB_LOGGER.set_file_name(app_log_name.c_str(), true, false, app_rs_log_name.c_str(),
                          app_ele_log_name.c_str(), app_trace_log_name.c_str());
}

void init_gtest_output(std::string &gtest_log_name)
{
  // 判断是否处于Farm中
  char *mit_network_start_port_env = getenv("mit_network_start_port");
  char *mit_network_port_num_env = getenv("mit_network_port_num");
  if (mit_network_start_port_env != nullptr && mit_network_port_num_env != nullptr) {
    std::string gtest_file_name = gtest_log_name;
    int fd = open(gtest_file_name.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd == 0) {
      ob_abort();
    }
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
  }
}

uint32_t get_local_addr(const char *dev_name)
{
  int fd, intrface;
  struct ifreq buf[16];
  struct ifconf ifc;

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return 0;
  }

  ifc.ifc_len = sizeof(buf);
  ifc.ifc_buf = (caddr_t)buf;
  if (ioctl(fd, SIOCGIFCONF, (char *)&ifc) != 0) {
    close(fd);
    return 0;
  }

  intrface = static_cast<int>(ifc.ifc_len / sizeof(struct ifreq));
  while (intrface-- > 0) {
    if (ioctl(fd, SIOCGIFFLAGS, (char *)&buf[intrface]) != 0) {
      continue;
    }
    if ((buf[intrface].ifr_flags & IFF_LOOPBACK) != 0)
      continue;
    if (!(buf[intrface].ifr_flags & IFF_UP))
      continue;
    if (dev_name != NULL && strcmp(dev_name, buf[intrface].ifr_name))
      continue;
    if (!(ioctl(fd, SIOCGIFADDR, (char *)&buf[intrface]))) {
      close(fd);
      return ((struct sockaddr_in *)(&buf[intrface].ifr_addr))->sin_addr.s_addr;
    }
  }
  close(fd);
  return 0;
}

std::string get_local_ip()
{
  uint32_t ip = get_local_addr("bond0");
  if (ip == 0) {
    ip = get_local_addr("eth0");
  }
  if (ip == 0) {
    return "";
  }
  return inet_ntoa(*(struct in_addr *)(&ip));
}

const char *ObMultiReplicaTestBase::log_disk_size_ = "10G";
const char *ObMultiReplicaTestBase::memory_size_ = "10G";
std::shared_ptr<observer::ObSimpleServerReplica> ObMultiReplicaTestBase::replica_ = nullptr;
bool ObMultiReplicaTestBase::is_started_ = false;
bool ObMultiReplicaTestBase::is_inited_ = false;
std::string ObMultiReplicaTestBase::env_prefix_;
std::string ObMultiReplicaTestBase::curr_dir_;
std::string ObMultiReplicaTestBase::env_prefix_path_;
std::string ObMultiReplicaTestBase::event_file_path_;
bool ObMultiReplicaTestBase::enable_env_warn_log_ = false;

std::vector<int64_t> ObMultiReplicaTestBase::rpc_ports_;
ObServerInfoList ObMultiReplicaTestBase::server_list_;
std::string ObMultiReplicaTestBase::rs_list_;

std::string ObMultiReplicaTestBase::local_ip_;
int ObMultiReplicaTestBase::child_pid_ = -1;
int ObMultiReplicaTestBase::child_pid2_ = -1;
int ObMultiReplicaTestBase::cur_zone_id_ = 0;

bool ObMultiReplicaTestBase::block_msg_ =  false;

ObMultiReplicaTestBase::ObMultiReplicaTestBase()
{
}

ObMultiReplicaTestBase::~ObMultiReplicaTestBase() {}

int ObMultiReplicaTestBase::bootstrap_multi_replica(const std::string &env_prefix)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    env_prefix_ =
        env_prefix + "_test_data"; //+ std::to_string(ObTimeUtility::current_time()) + "_";
    curr_dir_ = get_current_dir_name();
    env_prefix_path_ = curr_dir_ + "/" + env_prefix_;
    event_file_path_ = env_prefix_path_ + "/" + CLUSTER_EVENT_FILE_NAME;
    if (OB_FAIL(init_replicas_())) {
      SERVER_LOG(WARN, "init multi replica failed.", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!is_started_) {
    if (OB_FAIL(start())) {
      SERVER_LOG(WARN, "start multi replica failed.", KR(ret));
      sleep(5);
      abort();
    }
  }
  return ret;
}

int ObMultiReplicaTestBase::wait_all_test_completed()
{
  int ret = OB_SUCCESS;
  std::string zone_str = "ZONE" + std::to_string(cur_zone_id_);
  if (OB_FAIL(finish_event(TEST_CASE_FINSH_EVENT_PREFIX + zone_str, zone_str))) {

  } else {
    for (int i = 1; i <= MAX_ZONE_COUNT && OB_SUCC(ret); i++) {
      zone_str = "ZONE" + std::to_string(i);
      if (OB_FAIL(
              wait_event_finish(TEST_CASE_FINSH_EVENT_PREFIX + zone_str, zone_str, INT64_MAX))) {

        fprintf(stdout, "[WAIT EVENT] wait target event failed : ret = %d, zone_str = %s\n", ret,
                zone_str.c_str());
      }
    }
    SERVER_LOG(INFO, "ObMultiReplicaTestBase [WAIT EVENT] find all finish event", K(ret),
               K(cur_zone_id_), K(TEST_CASE_FINSH_EVENT_PREFIX));
    fprintf(stdout,
            "[WAIT EVENT] wait all test case successfully, ret = %d, cur_zone_id = %d, "
            "MAX_ZONE_COUNT = %d\n",
            ret, cur_zone_id_, MAX_ZONE_COUNT);
  }
  if (cur_zone_id_ == 1) {
    int status = 0;
    int status2 = 0;
    waitpid(child_pid_, &status, 0);
    waitpid(child_pid2_, &status2, 0);
    if (0 != status || 0 != status2) {
      fprintf(stdout,
              "Child process exit with error code : [%d]%d, [%d]%d\n",
              child_pid_, status, child_pid2_, status2);
      SERVER_LOG(INFO, "[ObMultiReplicaTestBase] Child process exit with error code", K(child_pid_),
                 K(status), K(child_pid2_), K(status2));
      ret = status;
      return ret;
    } else {
      fprintf(stdout,
              "Child process run all test cases done. [%d]%d, [%d]%d\n",
              child_pid_, status, child_pid2_, status2);
      SERVER_LOG(INFO, "[ObMultiReplicaTestBase] Child process run all test cases done",
                 K(child_pid_), K(status), K(child_pid2_), K(status2));
    }
  }
  return ret;
}

void ObMultiReplicaTestBase::SetUp()
{
  std::string cur_test_case_name = ::testing::UnitTest::GetInstance()->current_test_case()->name();
  std::string cur_test_info_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] SetUp", K(cur_test_case_name.c_str()),
             K(cur_test_info_name.c_str()));
}

void ObMultiReplicaTestBase::TearDown()
{
  std::string cur_test_case_name = ::testing::UnitTest::GetInstance()->current_test_case()->name();
  std::string cur_test_info_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] TearDown", K(cur_test_case_name.c_str()),
             K(cur_test_info_name.c_str()));
}

void ObMultiReplicaTestBase::SetUpTestCase()
{
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] SetUpTestCase");
}

void ObMultiReplicaTestBase::TearDownTestCase()
{
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] TearDownTestCase");

  int ret = OB_SUCCESS;

  // fprintf(stdout, ">>>>>>> AFTER RUN TEST: pid = %d\n", getpid());
  if (OB_FAIL(oceanbase::unittest::ObMultiReplicaTestBase::wait_all_test_completed())) {
    fprintf(stdout, "wait test case completed failed. ret = %d", ret);
  }
  if (OB_NOT_NULL(replica_)) {
    // ret = close();
    // ASSERT_EQ(ret, OB_SUCCESS);
  }
  int fail_cnt = ::testing::UnitTest::GetInstance()->failed_test_case_count();
  if (chdir(curr_dir_.c_str()) == 0) {
    bool to_delete = true;
    if (to_delete) {
      // system((std::string("rm -rf ") + env_prefix_ + std::string("*")).c_str());
    }
  }
  _Exit(fail_cnt);
}

int ObMultiReplicaTestBase::init_replicas_()
{
  SERVER_LOG(INFO, "init simple cluster test base");
  int ret = OB_SUCCESS;

  system(("rm -rf " + env_prefix_).c_str());

  SERVER_LOG(INFO, "create dir and change work dir start.", K(env_prefix_.c_str()));
  if (OB_FAIL(mkdir(env_prefix_.c_str(), 0777))) {
  } else if (OB_FAIL(chdir(env_prefix_.c_str()))) {
  } else {
    const char *current_dir = env_prefix_.c_str();
    SERVER_LOG(INFO, "create dir and change work dir done.", K(current_dir));
  }

  if (OB_SUCC(ret)) {
    local_ip_ = get_local_ip();
    if (local_ip_ == "") {
      SERVER_LOG(WARN, "get_local_ip failed");
      return -666666666;
    }
  }

  // mkdir
  std::vector<std::string> dirs;
  rs_list_.clear();
  rpc_ports_.clear();
  server_list_.reset();

  int server_fd = 0;
  for (int i = 1; i <= MAX_ZONE_COUNT && OB_SUCC(ret); i++) {
    std::string zone_dir = "zone" + std::to_string(i);
    ret = mkdir(zone_dir.c_str(), 0777);
    std::string data_dir = zone_dir + "/store";
    dirs.push_back(data_dir);
    dirs.push_back(zone_dir + "/run");
    dirs.push_back(zone_dir + "/etc");
    dirs.push_back(zone_dir + "/log");
    dirs.push_back(zone_dir + "/wallet");

    dirs.push_back(data_dir + "/clog");
    dirs.push_back(data_dir + "/slog");
    dirs.push_back(data_dir + "/sstable");

    int64_t tmp_port = observer::ObSimpleServerReplica::get_rpc_port(server_fd);
    rpc_ports_.push_back(tmp_port);

    rs_list_ += local_ip_ + ":" + std::to_string(rpc_ports_[i - 1]) + ":"
                + std::to_string(rpc_ports_[i - 1] + 1);

    if (i < MAX_ZONE_COUNT) {
      rs_list_ += ";";
    }

    obrpc::ObServerInfo server_info;
    server_info.zone_ = zone_dir.c_str();
    server_info.server_ =
        common::ObAddr(common::ObAddr::IPV4, local_ip_.c_str(), rpc_ports_[i - 1]);
    server_info.region_ = "sys_region";
    server_list_.push_back(server_info);
  }

  if (OB_SUCC(ret)) {
    for (auto &dir : dirs) {
      ret = mkdir(dir.c_str(), 0777);
      if (OB_FAIL(ret)) {
        SERVER_LOG(ERROR, "ObSimpleServerReplica mkdir", K(ret), K(dir.c_str()));
        return ret;
      }
    }
  }

  if (OB_SUCC(ret)) {

    child_pid_ = fork();
    child_pid2_ = -1;

    if (child_pid_ < 0) {
      perror("fork");
      exit(EXIT_FAILURE);
    } else if (child_pid_ > 0) {
      child_pid2_ = fork();
      if (child_pid2_ > 0) {
        ret = init_test_replica_(1);
      } else if (child_pid2_ == 0) {
        ret = init_test_replica_(3);
      }
    } else if (child_pid_ == 0) {
      ret = init_test_replica_(2);
    }
  }

  is_inited_ = true;
  return ret;
}

int ObMultiReplicaTestBase::init_test_replica_(const int zone_id)
{
  int ret = OB_SUCCESS;

  ::testing::GTEST_FLAG(filter) = ObMultiReplicaTestBase::ZONE_TEST_CASE_NAME[zone_id - 1] + "*";
  // std::string output_file_path =
      // env_prefix_path_ + "/" + "ZONE" + std::to_string(zone_id) + ".output";
  // ::testing::GTEST_FLAG(output) = output_file_path;
  fprintf(stdout, "zone %d test_case_name = %s\n", zone_id,
          ObMultiReplicaTestBase::ZONE_TEST_CASE_NAME[zone_id - 1].c_str());

  if (replica_ == nullptr) {
    cur_zone_id_ = zone_id;
    replica_ = std::make_shared<observer::ObSimpleServerReplica>(
        "zone" + std::to_string(zone_id), zone_id, rpc_ports_[zone_id - 1], rs_list_, server_list_,
        oceanbase::observer::ObServer::get_instance(), "./store", log_disk_size_, memory_size_);
  } else {
    SERVER_LOG(ERROR, "construct ObSimpleServerReplica repeatedlly", K(ret), K(zone_id),
               K(rpc_ports_[zone_id - 1]), K(rs_list_.c_str()));
  }

  if (replica_ != nullptr) {
    int ret = replica_->simple_init();
    if (OB_FAIL(ret)) {
      SERVER_LOG(ERROR, "init replica failed", K(ret), K(zone_id));
    }
  }
  return ret;
}

int ObMultiReplicaTestBase::read_cur_json_document_(rapidjson::Document &json_doc)
{
  int ret = OB_SUCCESS;
  FILE *fp = fopen(event_file_path_.c_str(), "r");
  if (fp == NULL) {
    if(json_doc.IsObject())
    {
    fprintf(stdout, "Fail to open file! file_path = %s\n", event_file_path_.c_str());
    }
    ret = OB_ENTRY_NOT_EXIST;
    return ret;
  }

  char read_buffer[2 * 1024 * 1024];
  rapidjson::FileReadStream rs(fp, read_buffer, sizeof(read_buffer));

  json_doc.ParseStream(rs);

  fclose(fp);

  return OB_SUCCESS;
}

int ObMultiReplicaTestBase::wait_event_finish(const std::string &event_name,
                                              std::string &event_content,
                                              int64_t wait_timeout_ms,
                                              int64_t retry_interval_ms)
{
  int ret = OB_SUCCESS;

  bool find_event = false;
  int64_t start_time = ObTimeUtility::fast_current_time();

  while (OB_SUCC(ret) && !find_event) {

    rapidjson::Document json_doc;

    if (OB_FAIL(read_cur_json_document_(json_doc))) {
      SERVER_LOG(WARN, "read existed json document failed", K(ret));
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_SUCCESS;
      }
    } else {
      rapidjson::Value::ConstMemberIterator iter = json_doc.FindMember(event_name.c_str());
      if (iter == json_doc.MemberEnd()) {

        SERVER_LOG(WARN, "[ObMultiReplicaTestBase] [WAIT EVENT] not find target event", K(ret),
                   K(event_name.c_str()));
        ret = OB_SUCCESS;
      } else {
        find_event = true;
        event_content = std::string(iter->value.GetString(), iter->value.GetStringLength());
        fprintf(stdout, "[WAIT EVENT] find target event : EVENT_KEY = %s; EVENT_VAL = %s\n",
                event_name.c_str(), iter->value.GetString());
        SERVER_LOG(INFO, "[ObMultiReplicaTestBase] [WAIT EVENT] find target event",
                   K(event_name.c_str()), K(iter->value.GetString()));
      }
    }

    if (!find_event) {
      if (wait_timeout_ms != INT64_MAX
          && ObTimeUtility::fast_current_time() - start_time > wait_timeout_ms * 1000) {
        ret = OB_TIMEOUT;
        break;
      } else {
        ob_usleep(retry_interval_ms * 1000);
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObMultiReplicaTestBase::finish_event(const std::string &event_name,
                                         const std::string &event_content)
{
  int ret = OB_SUCCESS;

  rapidjson::Document json_doc;
  json_doc.Parse("{}");

  if (OB_FAIL(read_cur_json_document_(json_doc))) {
    SERVER_LOG(WARN, "read existed json document failed", K(ret));
    if (ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    FILE *fp = fopen(event_file_path_.c_str(), "w");
    char write_buffer[2 * 1024 * 1024];
    rapidjson::FileWriteStream file_w_stream(fp, write_buffer, sizeof(write_buffer));
    rapidjson::PrettyWriter<rapidjson::FileWriteStream> prettywriter(file_w_stream);
    json_doc.AddMember(rapidjson::StringRef(event_name.c_str(), event_name.size()),
                       rapidjson::StringRef(event_content.c_str(), event_content.size()),
                       json_doc.GetAllocator());
    json_doc.Accept(prettywriter);
    fclose(fp);
  }

  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] [WAIT EVENT] write target event", K(event_name.c_str()),
             K(event_content.c_str()));
  return ret;
}

int ObMultiReplicaTestBase::start()
{
  SERVER_LOG(INFO, "start simple cluster test base");
  OB_LOGGER.set_enable_log_limit(false);
  // oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE();
  // oceanbase::palf::election::INIT_TS = 1;
  // oceanbase::palf::election::MAX_TST = 100 * 1000;
  GCONF.enable_perf_event = false;
  GCONF.enable_sql_audit = true;
  GCONF.enable_record_trace_log = false;

  int32_t log_level;
  bool change_log_level = false;
  if (enable_env_warn_log_) {
    if (OB_LOGGER.get_log_level() > OB_LOG_LEVEL_WARN) {
      change_log_level = true;
      log_level = OB_LOGGER.get_log_level();
      OB_LOGGER.set_log_level("WARN");
    }
  }

  int ret = replica_->simple_start();
  is_started_ = true;
  if (change_log_level) {
    OB_LOGGER.set_log_level(log_level);
  }
  return ret;
}

int ObMultiReplicaTestBase::close()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(replica_)) {
    ret = replica_->simple_close();
  }
  return ret;
}

int ObMultiReplicaTestBase::create_tenant(const char *tenant_name,
                                          const char *memory_size,
                                          const char *log_disk_size,
                                          const bool oracle_mode)
{
  SERVER_LOG(INFO, "create tenant start");
  int32_t log_level;
  bool change_log_level = false;
  if (enable_env_warn_log_) {
    if (OB_LOGGER.get_log_level() > OB_LOG_LEVEL_WARN) {
      change_log_level = true;
      log_level = OB_LOGGER.get_log_level();
      OB_LOGGER.set_log_level("WARN");
    }
  }
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = replica_->get_sql_proxy();
  int64_t affected_rows = 0;
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("set session ob_trx_timeout=1000000000000;"))) {
      SERVER_LOG(WARN, "set session", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "set session", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("set session ob_query_timeout=1000000000000;"))) {
      SERVER_LOG(WARN, "set session", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "set session", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("create resource unit box_ym_%s max_cpu 2, memory_size '%s', "
                                      "log_disk_size='%s';",
                                      tenant_name, memory_size, log_disk_size))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("create resource pool pool_ym_%s unit = 'box_ym_%s', "
                                      "unit_num = 1, zone_list = ('zone1', 'zone2', 'zone3');",
                                      tenant_name, tenant_name))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt(
                   "create tenant %s replica_num = 3, primary_zone='zone1', "
                   "resource_pool_list=('pool_ym_%s') set ob_tcp_invited_nodes='%%'%s",
                   tenant_name, tenant_name,
                   oracle_mode ? ", ob_compatibility_mode='oracle'" : ";"))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (FAILEDx(sql.assign_fmt("alter system set _enable_parallel_table_creation = false tenant = all"))) {
      SERVER_LOG(WARN, "create_tenant", KR(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "create_tenant", KR(ret));
    }
    usleep(5 * 1000 * 1000L); // 5s
  }
  if (change_log_level) {
    OB_LOGGER.set_log_level(log_level);
  }
  SERVER_LOG(INFO, "create tenant finish", K(ret));
  return ret;
}

int ObMultiReplicaTestBase::delete_tenant(const char *tenant_name)
{
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = replica_->get_sql_proxy();
  sql.assign_fmt("drop tenant %s force", tenant_name);

  int64_t affected_rows = 0;
  return sql_proxy.write(sql.ptr(), affected_rows);
}

int ObMultiReplicaTestBase::get_tenant_id(uint64_t &tenant_id, const char *tenant_name)
{
  SERVER_LOG(INFO, "get_tenant_id");
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = replica_->get_sql_proxy();
  sql.assign_fmt("select tenant_id from oceanbase.__all_tenant where tenant_name = '%s'",
                 tenant_name);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      SERVER_LOG(WARN, "get_tenant_id", K(ret));
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result != nullptr && OB_SUCC(result->next())) {
        ret = result->get_uint("tenant_id", tenant_id);
        SERVER_LOG(WARN, "get_tenant_id", K(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "get_tenant_id", K(ret));
      }
    }
  }
  return ret;
}

int ObMultiReplicaTestBase::exec_write_sql_sys(const char *sql_str, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  return sql_proxy.write(sql_str, affected_rows);
}

int ObMultiReplicaTestBase::check_tenant_exist(bool &bool_ret, const char *tenant_name)
{
  int ret = OB_SUCCESS;
  bool_ret = true;
  uint64_t tenant_id;
  if (OB_FAIL(get_tenant_id(tenant_id, tenant_name))) {
    SERVER_LOG(WARN, "get_tenant_id failed", K(ret));
  } else {
    ObSqlString sql;
    common::ObMySQLProxy &sql_proxy = replica_->get_sql_proxy();
    sql.assign_fmt("select tenant_id from oceanbase.gv$ob_units where tenant_id= '%" PRIu64 "' ",
                   tenant_id);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "get gv$ob_units", K(ret));
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        if (result != nullptr && OB_SUCC(result->next())) {
          bool_ret = true;
        } else if (result == nullptr) {
          bool_ret = false;
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "get_tenant_id", K(ret));
        }
      }
    }
  }
  return ret;
}


} // namespace unittest
} // namespace oceanbase


int ::oceanbase::omt::ObWorkerProcessor::process_err_test()
{
  int ret = OB_SUCCESS;

  if(ATOMIC_LOAD(&::oceanbase::unittest::ObMultiReplicaTestBase::block_msg_))
  {
    ret =OB_EAGAIN;
    SERVER_LOG(INFO, "[ObMultiReplicaTestBase] block msg process",K(ret));
  }

  return ret;
}
