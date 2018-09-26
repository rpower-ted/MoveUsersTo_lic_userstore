using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Reflection;

using K3NET.Strings;
using RPData.Database.Common;
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
                                userStoreList.AddRange(storeList.FindAll(delegate (Store s) { return s.SerialNumber == serialNumber; }));
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

                }
            }
            catch (DbException)
            {

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
        internal RawOperationData GenerateMultiRowInsertOnDuplicateKeyUpdateSQL(List<RecordData> recordList,
                                                                                ReplicationTable t,
                                                                                DatabaseManager.eDatabaseType dbType,
                                                                                bool changeTimeStamps,
                                                                                Constants.ReplType replicationType)
        {
            var isg = new SQLInsertStatement();

            var qo = new RawOperationData(dbType);
            var tc = new TableClause(t.SourceTable);
            var fieldList = new List<ClauseItem>();

            // this is where we get our column names for the insert statement
            foreach (Column col in t.DestTable.Columns)
            {
                // Skip the time_stamp field. We no longer import a value for it. time_stamp is now 
                // of type TIMESTAMP, and TIMESTAMP fields initialize themselves to the current UTC time, but
                // display as local time. When a record is updated, the value of a TIMESTAMP field is automatically
                // updated, so no need to pass a value to them.  -- Ted 2018-06-16

                // if (col.Name.Equals(Constants.TimeStamp))
                // if we don't want to change time stamps, then we need to add the time_stamp field
                // to push the current value to the database. 
                // if we do want to change timestamps, then we don't want to add the field because
                // it will automatically be given a value by the database when added to the database.
                if (col.Name.Equals(Constants.TimeStamp) && changeTimeStamps)
                    continue;

                fieldList.Add(new ClauseItem(new FieldName(this.BookendString(col.Name, dbType)),
                                             operation: ClauseItem.Operation.Include));
            }


            var recItemList = new List<ClauseItem>();

            // this is where we create our value list for the insert statement
            foreach (RecordData rd in recordList)
            {
                ClauseItem item = new ClauseItem();
                var valueList = new List<ClauseItem>();

                foreach (KeyValuePair<string, RecordDataField> kvp in rd.AllFields)
                {
                    // if this is the time_stamp field and we want to allow timestamps to change,
                    // then ignore this column.
                    if (kvp.Key.Equals(Constants.TimeStamp) && changeTimeStamps)
                        continue;

                    // column name translations from old to new

                    // skip if the record contains both rid and record_id fields
                    if (kvp.Key.Equals(QueryBuilder.RID_Column) &&
                        rd.AllFields.ContainsKey(QueryBuilder.RecordId_Column))
                        continue;

                    if (kvp.Key.Equals(QueryBuilder.RecordId_Column))
                    {
                        valueList.Add(new ClauseItem(compValue: kvp.Value.Value));
                        continue;
                    }
                    else if (kvp.Key.Equals(QueryBuilder.MIDFlags_Column))
                    {
                        valueList.Add(new ClauseItem(compValue: kvp.Value.Value));
                        continue;
                    }
                    else if (kvp.Key.Equals(QueryBuilder.MIDTimeStamp_Column))
                        continue; // ignore this column

                    Column c = t.DestTable.GetColumn(kvp.Key);

                    if (c == null)
                        continue;

                    if (kvp.Key.Equals(Replicator.TimeStampFieldName))
                    {
                        Column srcCol = t.SourceTable.GetColumn(Replicator.TimeStampFieldName);

                        if (srcCol.DataType == Column.FIELD_TYPE_TIMESTAMP)
                        {

                        }
                        else
                        {
                            DateTime dt = Convert.ToDateTime(kvp.Value.Value);
                            valueList.Add(new ClauseItem(compValue: dt));
                        }

                        continue;
                    }

                    if (c != null && c.Type.Equals(QueryBuilder.DateTime_Type))
                    {
                        DateTime dt = Convert.ToDateTime(kvp.Value.Value.ToString());
                        valueList.Add(new ClauseItem(compValue: dt));
                    }
                    else
                    {
                        if (kvp.Value.Value.ToString().Equals(Column.DEFAULT_NULL))
                        {
                            valueList.Add(new ClauseItem(compValue: SQLStatementGenerator.Explicit_NULL_Indicator));
                        }
                        else
                        {
                            valueList.Add(new ClauseItem(compValue: kvp.Value.Value));
                        }
                    }
                } // end  forach (KeyValuePair<string, RecordDataField> kvp in rd.AllFields)

                // hasha 
                if (t.DestTable.DoesContainColumn(QueryBuilder.HashA_Column) &&
                    !t.SourceTable.DoesContainColumn(QueryBuilder.HashA_Column) &&
                    !t.DestTable.DoesContainColumn(QueryBuilder.HashB_Column))
                {
                    long hasha = DataUtilities.Synthesize63BitStrashFromFieldList(t.DestTable.HashAFields, rd, t.DestTable);
                    bool hashaExists = fieldList.Exists(delegate (ClauseItem ci) { return ci.Value.ToString().Equals(QueryBuilder.HashA_Column); });

                    if (!hashaExists)
                        fieldList.Add(new ClauseItem(operation: ClauseItem.Operation.Include, compValue: this.BookendString(QueryBuilder.HashA_Column, dbType)));

                    valueList.Add(new ClauseItem(compValue: hasha));
                }

                // mid
                if (t.DestTable.DoesContainColumn(QueryBuilder.MID_Column) &&
                   !t.SourceTable.DoesContainColumn(QueryBuilder.MID_Column))
                {
                    bool midColExists = fieldList.Exists(delegate (ClauseItem ci) { return ci.Value.ToString().Equals(QueryBuilder.MID_Column); });

                    if (!midColExists)
                        fieldList.Add(new ClauseItem(operation: ClauseItem.Operation.Include, compValue: this.BookendString(QueryBuilder.MID_Column, dbType)));

                    valueList.Add(new ClauseItem(compValue: DataUtilities.GeneratePUN()));
                }

                item.Value = valueList;
                recItemList.Add(item);
            }

            qo.StageList.Add(1, new Stage(new ClauseItem(compValue: tc), Stage.eOperation.Insert));
            qo.StageList.Add(2, new Stage(fieldList, Stage.eOperation.Project));
            qo.StageList.Add(3, new Stage(recItemList, Stage.eOperation.Values));

            if (replicationType == Constants.ReplType.Live)
            {
                // this is where we generate the On Duplicate Key clause
                var updateItemList = new List<ClauseItem>();

                foreach (Column c in t.DestTable.Columns)
                {
                    if (c.Name.Equals(Replicator.TimeStampFieldName))
                        continue;

                    updateItemList.Add(new ClauseItem(new FieldName(c.Name), operation: ClauseItem.Operation.Equals, compValue: new FieldName(c.Name)));
                }

                qo.StageList.Add(4, new Stage(updateItemList, Stage.eOperation.OnDuplicateKey));
            }


            isg.Fill(qo);

            return qo;
        }
    }
}
