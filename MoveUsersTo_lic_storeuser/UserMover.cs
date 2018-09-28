using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Reflection;

using K3NET.Strings;
using RPData.Database;
using RPData.Database.Common;
using RPData.DML;
using RPData.DML.SQL;
using RPNET.IO;

namespace MoveUsersTo_lic_storeuser
{
    /// <summary>
    /// This class moves user records from tbl_user to lic_storeuser
    /// </summary>
    public class UserMover
    {
        private const string IniDirectoryName = "ini";
        private const string      LogFileName = "MoveUsersTo_lic_storeuser.log";
        private const string TblStoreUserName = "tbl_storeuser";
        private const string         MidField = "mid";
        private const string     UserMidField = "user_mid";
        private const string    StoreMidField = "store_mid";
        
        /// <summary>
        /// The query builder
        /// </summary>
        private QueryBuilder QueryBuilder { get; set; }
     
        /// <summary>
        /// Constructor
        /// </summary>
        public UserMover()
        {
            this.QueryBuilder = new QueryBuilder();
            DataUtilities.InitializePunSpinner();
        }

        /// <summary>
        /// Moves users from tbl_user to lic_storeuser
        /// </summary>
        public void Run()
        {
            var log = new LogFile(this.GetLogFilePath());
            
            bool hasINIFile = this.CreateINIFile(this.GetExecutableFolderPath(), this.GetINIFileName());

            if (!hasINIFile)
            {
                log.AppendStatement("No ini file.");
                return;
            }
                
            Preferences prefs = this.ReadPreferences(this.GetINIFilePath(this.GetINIFileName()));
            var     dbManager = new DatabaseManagerChooser(prefs.DatabaseSettings, log.FilePath);
            var           csb = new ConnectionStringBuilder(prefs.DatabaseSettings);
            
            try
            {
                dbManager.OpenConnection(csb.GetConnectionStringRpowerRootPassword());
                List<User> userList = this.RetrieveUserList(dbManager, prefs.EarliestTimeStamp);

                if (userList.Count == 0)
                    return;

                List<Store> storeList = this.RetrieveStoreList(dbManager);

                if (storeList.Count == 0)
                    return;

                foreach(User user in userList)
                {
                    var midList = new List<long>();

                   if (user.CGList.Count > 0)
                    {
                        var userStoreList = new List<Store>();

                        if (user.StoreSNList.Count > 0)
                        {
                            foreach (int serialNumber in user.StoreSNList)
                            {
                                userStoreList.AddRange(storeList.FindAll(delegate (Store s) 
                                                                           { return s.SerialNumber == serialNumber; }));
                            }
                        }
                        else
                        {
                            foreach (int cg in user.CGList)
                            {
                                userStoreList.AddRange(storeList.FindAll(delegate (Store s) { return s.CG == cg; }));
                            }
                        }

                        foreach (Store store in userStoreList)
                        {
                            user.StoreMidList.Add(store.Mid);
                        }
                    }                   
                } 
                
                foreach (User user in userList)
                {
                    RawOperationData rod = this.GenerateMultiRowInsertOnDuplicateKeyUpdateSQL(user);
                    OperationResult   or = dbManager.Insert(rod);

                    if (or.DidSucceed == false)
                    {
                        log.Append("Error inserting records: " + or.ErrorMessage + " on " + or.Statement);
                    }
                }
            }
            catch (DbException error)
            {
                log.AppendStatement(error.Message);
            }         
            finally
            {
                dbManager.CloseConnection();
            }
        }

        /// <summary>
        /// Generates the path to the log file
        /// </summary>
        /// <returns></returns>
        private string GetLogFilePath()
        {
            return Path.Combine(this.GetExecutableFolderPath(), UserMover.LogFileName);
        }

        /// <summary>
        /// Builds a file path under the executable directory
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        private string GetINIFilePath(string fileName)
        {
            return GetExecutableFolderPath() + fileName;
        }

        /// <summary>
        /// Gets a path to the executable directory
        /// </summary>
        /// <returns></returns>
        private string GetExecutableFolderPath()
        {
            return AppDomain.CurrentDomain.BaseDirectory;
        }

        private bool CreateINIFile(string directoryPath, string fileName)
        {
            string iniDirectoryPath = this.GetExecutableFolderPath() + UserMover.IniDirectoryName;

            try
            {
                if (!Directory.Exists(iniDirectoryPath))
                    Directory.CreateDirectory(iniDirectoryPath);

                string iniFilePath = Path.Combine(new string[] { iniDirectoryPath, fileName });

                if (!File.Exists(iniFilePath))
                {
                    File.Create(iniFilePath);
                }
            }
            catch
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// gets the product name from the assembly
        /// </summary>
        /// <returns></returns>
        private string GetINIFileName()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var      fvi = FileVersionInfo.GetVersionInfo(assembly.Location);

            return fvi.ProductName;
        }

        /// <summary>
        /// Reads the preferences out of the MoveUsersTo_lic_storeuser.ini file
        /// </summary>
        /// <param name="iniFilePath"></param>
        /// <returns></returns>
        private Preferences ReadPreferences(string iniFilePath)
        {
            var preferences = new Preferences();

            preferences.Read(iniFilePath);

            return preferences;
        }

        /// <summary>
        /// Retrieves a list of users from tbl_users
        /// </summary>
        /// <param name="dbmc"></param>
        /// <param name="earliestTimeStamp"></param>
        /// <returns></returns>
        private List<User> RetrieveUserList(DatabaseManagerChooser dbmc, DateTime earliestTimeStamp)
        {
            var     userList = new List<User>();
            string     query = this.QueryBuilder.GetUsersWithTimeStamps(earliestTimeStamp);
            DbDataReader dbr = null;

            try
            {                
                dbr = dbmc.ExecuteDataReader(query);

                if (!dbr.HasRows)
                    return userList;

                while (dbr.Read())
                {

                    string username  = dbr.GetString(0);
                    string cgListStr = dbr.GetString(1);
                    string storeList = dbr.GetString(2);

                    if (string.IsNullOrEmpty(username)  ||
                        string.IsNullOrEmpty(cgListStr) ||
                        string.IsNullOrEmpty(storeList))
                    {
                        continue;
                    }

                    var      user    = new User();
                    user.UserName    = username;
                    user.CGList      = this.GenerateIntList(cgListStr);                    
                    user.StoreSNList = this.GenerateIntList(storeList);
                    user.Mid         = dbr.GetInt64(3);
                    
                    userList.Add(user);
                }
            }
            catch
            {

            }
            finally
            {
                if (dbr != null)
                {
                    if (!dbr.IsClosed)
                        dbr.Close();
                }
            }

            return userList;
        }

        public List<int> GenerateIntList(string pdStr)
        {
            if (pdStr.Equals("4"))
            {
                return new List<int>();
            }

            var             storeSNList = new List<int>();
            List<string> storeSNStrList = StringUtilities.GenerateStringList(pdStr, '|');

            foreach (string storeSNStr in storeSNStrList)
            {
                try
                {
                    int i = Convert.ToInt32(storeSNStr);
                    storeSNList.Add(i);
                }
                catch
                {

                }                
            }

            return storeSNList;
        }

        /// <summary>
        /// Retrieves a list of stores from tbl_store
        /// </summary>
        /// <param name="dbmc"> a fully initialized DatabaseManagerChooser </param>
        /// <returns> a List of Store objects </returns>
        public List<Store> RetrieveStoreList(DatabaseManagerChooser dbmc)
        {
            var       storeList = new List<Store>();
            string        query = this.QueryBuilder.GetStores();
            DbDataReader reader = null;

            try
            {
                reader = dbmc.ExecuteDataReader(query);

                if (!reader.HasRows)
                    return storeList;

                while (reader.Read())
                {
                    var store = new Store();

                    store.CG           = reader.GetInt32(0);
                    store.SerialNumber = reader.GetInt32(1);
                    store.Mid          = reader.GetInt64(2);

                    storeList.Add(store);
                }                
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    if (!reader.IsClosed)
                    {
                        reader.Close();
                    }
                }
            }
                        
            return storeList;
        }

        /// <summary>
        /// Generates a multi-record insert...on duplicate key update statement
        /// </summary>
        /// <param name="recordList"> a list of RecordData objects </param>
        /// <param name="t"> a Table object </param>
        /// <param name="dbType"> an enumerated value indicating the database type </param>
        /// <returns></returns>
        internal RawOperationData GenerateMultiRowInsertOnDuplicateKeyUpdateSQL(User user)
        {
            var isg = new SQLInsertStatement();

            var  destTable = new Table();
            destTable.Name = UserMover.TblStoreUserName;

            var        tc = new TableClause(destTable);
            var        qo = new RawOperationData(DatabaseManager.eDatabaseType.MySQL);

            var fieldList = new List<ClauseItem>();

            fieldList.Add(new ClauseItem(new FieldName(UserMover.MidField), operation: ClauseItem.Operation.Include));
            fieldList.Add(new ClauseItem(new FieldName(UserMover.UserMidField), operation: ClauseItem.Operation.Include));
            fieldList.Add(new ClauseItem(new FieldName(UserMover.StoreMidField), operation: ClauseItem.Operation.Include));


            var recItemList = new List<ClauseItem>();
            var   valueList = new List<ClauseItem>();
            
            foreach(long storeMid in user.StoreMidList)
            {
                var item = new ClauseItem();
                valueList.Add(new ClauseItem(operation: ClauseItem.Operation.Include, compValue: DataUtilities.GeneratePUN()));
                valueList.Add(new ClauseItem(operation: ClauseItem.Operation.Include, compValue: user.Mid));
                valueList.Add(new ClauseItem(operation: ClauseItem.Operation.Include, compValue: user.StoreMidList));

                item.Value = valueList;
                recItemList.Add(item);
            }
                                                       
            qo.StageList.Add(1, new Stage(new ClauseItem(compValue: tc), Stage.eOperation.InsertIgnore));
            qo.StageList.Add(2, new Stage(fieldList, Stage.eOperation.Project));
            qo.StageList.Add(3, new Stage(recItemList, Stage.eOperation.Values));

            isg.Fill(qo);

            return qo;
        }
    }
}
