using System;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Reflection;

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
        /// Constructor
        /// </summary>
        public UserMover()
        {

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

            var csb = new ConnectionStringBuilder(prefs.DatabaseSettings);

            try
            {
                dbManager.OpenConnection(csb.GetConnectionStringRpowerRootPassword());
                var queryBuilder = new QueryBuilder();

                string query = 

                dbManager.ExecuteDataReader()
            }
            catch (DbException)
            {

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

    }
}
