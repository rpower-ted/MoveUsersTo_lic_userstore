using System;

using RPNET.IO;

using RPData.Database.Common;

namespace MoveUsersTo_lic_storeuser
{
    public class Preferences
    {
        private const string PreferencesSectionKey = "Preferences";
        private const string           DatabaseKey = "Database";
        private const string  EarliestTimeStampKey = "EarliestTimeStamp";
        /// <summary>
        /// The earliest timestamp we are looking for when pulling user records from tbl_users
        /// </summary>
        public DateTime EarliestTimeStamp { get; private set; }

        /// <summary>
        /// Gets the database connection settings
        /// </summary>
        public DatabaseManagerSettings DatabaseSettings { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public Preferences()
        {

        }

        /// <summary>
        /// Reads settings from the application ini file
        /// </summary>
        /// <param name="iniFilePath"> the path to the application's ini file </param>
        /// <returns> a boolean indicating whether the preferences have been read </returns>
        public bool Read(string iniFilePath)
        {
           var iniFile = new INIFile(iniFilePath);

            string  databaseString = iniFile.ReadValue(Preferences.PreferencesSectionKey, Preferences.DatabaseKey);
            string timeStampString = iniFile.ReadValue(Preferences.PreferencesSectionKey, Preferences.EarliestTimeStampKey);

            this.DatabaseSettings  = this.ConvertToDatabaseSettings(databaseString);

            if (String.IsNullOrEmpty(timeStampString))
            {
                this.EarliestTimeStamp = new DateTime(2000, 1, 1);
            }                
            else
            {
                this.EarliestTimeStamp = Convert.ToDateTime(timeStampString);
            }

            return true;
        }

        private DatabaseManagerSettings ConvertToDatabaseSettings(string str)
        {
            string[] infoArray = str.Split(new char[] { '|' });

            return new DatabaseManagerSettings(infoArray[2], 
                                               infoArray[1], 
                                               string.Empty, 
                                               DatabaseManager.eDatabaseType.MySQL);
        }

    }
}
