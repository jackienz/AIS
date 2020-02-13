using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.IO;

namespace DAL
{
    /// <summary>
    /// 通用数据访问类
    /// </summary>
    class SQLHelper
    {
        private static string connString = ConfigurationManager.ConnectionStrings["connString"].ToString();

        #region 执行格式化的SQL语句
        /// <summary>
        /// 执行增,删,改操作
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static int Update(string sql)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                return cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行Update(string sql)方法时发生错误,具体信息:" + ex.Message);
                throw;
            }
            finally
            {
                conn.Close();
            }
        }
        /// <summary>
        /// 获取单一结果
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static object GetSingleResult(string sql)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                return cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行GetSingleResult(string sql)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }
        /// <summary>
        /// 执行返回结果集的查询(只读结果集)
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static SqlDataReader GetReader(string sql)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行GetSingleResult(string sql)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
        }
        /// <summary>
        /// 执行返回数据集的查询
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static DataSet GetDataSet(string sql)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();
            try
            {
                conn.Open();
                da.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行GetDataSet(string sql)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }
        /// <summary>
        /// 同时执行多条查询语句,并将结果填充到对应的DataTable里面,读取时可以根据表的名称访问DataTable
        /// </summary>
        /// <param name="sqlDic">使用HashTable类型的泛型集合封装对一个的SQL语句的数据表名称</param>
        /// <returns></returns>
        public static DataSet GetDataSet(Dictionary<string, string> sqlDic)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();
            try
            {
                conn.Open();
                foreach (string tableName in sqlDic.Keys)
                {
                    cmd.CommandText = sqlDic[tableName];
                    da.Fill(ds, tableName);
                }
                return ds;
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行GetDataSet(string sql)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }
        /// <summary>
        /// 基于ADO.NET事务提交多条增删改SQL语句
        /// </summary>
        /// <param name="sqlList">sql语句集合</param>
        /// <returns>返回是否执行成功</returns>
        public static bool UpdateByTran(List<string> sqlList)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;
            try
            {
                conn.Open();
                cmd.Transaction = conn.BeginTransaction();//开启事务
                //以循环的方式提交SQL语句
                foreach (string item in sqlList)
                {
                    cmd.CommandText = item;
                    cmd.ExecuteNonQuery();
                }
                cmd.Transaction.Commit();//提交事务(同时自动清除事务)
                return true;
            }
            catch (Exception ex)
            {
                if (cmd.Transaction != null)
                {
                    cmd.Transaction.Rollback();//回滚事务
                }
                //将错误信息写入日志文件
                WriteErrorLog("执行UpdateByTran(List<string> sqlList)方法时发生错误,具体信息:" + ex.Message);
                throw new Exception("调用事务方法UpdateByTran(List<string> sqlList)时出现错误:" + ex.Message);
            }
            finally
            {
                if (cmd.Transaction != null)
                {
                    cmd.Transaction = null;
                }
                conn.Close();
            }
        }

        #endregion


        #region 执行带参数的SQL语句
        /// <summary>
        /// 执行带参数的增删改操作
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static int Update(string sql, SqlParameter[] param)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                cmd.Parameters.AddRange(param);//添加参数
                return cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行Update(string sql,SqlParameter[] param)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// 获取单一结果
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static object GetSingleResult(string sql, SqlParameter[] param)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                cmd.Parameters.AddRange(param);
                return cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行GetSingleResult(string sql,SqlParameter[] param)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }
        /// <summary>
        /// 执行返回结果集的查询(只读结果集)
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static SqlDataReader GetReader(string sql, SqlParameter[] param)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                cmd.Parameters.AddRange(param);
                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行GetReader(string sql,SqlParameter[] param)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
        }

        #endregion

        #region 调用存储过程

        public static int UpdateByProcedure(string storeProcedureName, SqlParameter[] param)
        {
            SqlConnection conn = new SqlConnection(connString);
            // SqlCommand cmd = new SqlCommand(storeProcedureName, conn);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;
            cmd.CommandType = CommandType.StoredProcedure;//告诉command对象,当前的操作是执行存储过程
            cmd.CommandText = storeProcedureName;
            try
            {
                conn.Open();
                cmd.Parameters.AddRange(param);//添加参数
                return cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行UpdateByProcedure(string storeProcedureName, SqlParameter[] param)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }


        public static SqlDataReader GetReaderByProcedure(string storeProcedureName, SqlParameter[] param)
        {
            SqlConnection conn = new SqlConnection(connString);
            //SqlCommand cmd = new SqlCommand(sql, conn);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;
            cmd.CommandType = CommandType.StoredProcedure;//告诉command对象,当前的操作是执行存储过程
            cmd.CommandText = storeProcedureName;
            try
            {
                conn.Open();
                if (param != null)
                {
                    cmd.Parameters.AddRange(param);
                }

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行GetReaderByProcedure(string storeProcedureName, SqlParameter[] param)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
        }

        public static DataSet GetDataSetByProcedure(string storeProcedureName, SqlParameter[] param)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(storeProcedureName, conn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();
            try
            {
                conn.Open();
                if (param!=null)
                {
                    cmd.Parameters.AddRange(param);
                }
                da.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                //将错误信息写入日志文件
                WriteErrorLog("执行GetDataSet(string sql)方法时发生错误,具体信息:" + ex.Message);
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }

        #endregion


        #region 获取数据库服务器的时间、将错误信息写入日志
        /// <summary>
        /// 获取数据库服务器的时间
        /// </summary>
        /// <returns></returns>
        public static DateTime GetDBServerime()
        {
            string sql = "select getdate()";
            return Convert.ToDateTime(GetSingleResult(sql));
        }

        /// <summary>
        /// 将错误信息写入日志
        /// </summary>
        /// <param name="msg"></param>
        private static void WriteErrorLog(string msg)
        {
            FileStream fs = new FileStream("ProjectDemo.log", FileMode.Append);
            StreamWriter sw = new StreamWriter(fs);
            sw.WriteLine("[" + DateTime.Now.ToString() + "]错误信息:" + msg);
            sw.Close();
            fs.Close();
        }
        #endregion
    }
}
