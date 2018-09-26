using System;
using System.Collections.Generic;

using K3NET.Strings;

namespace MoveUsersTo_lic_storeuser
{
    /// <summary>
    /// Builds queries for this application
    /// </summary>
    public class QueryBuilder
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public QueryBuilder()
        {

        }

        /// <summary>
        /// Builds a query to retrieve users whose records have a timestamp later or equal to the incoming datetime
        /// </summary>
        /// <param name="earliestTimeStamp"> the earliest timestamp that we want user records for </param>
        /// <returns></returns>
        public string GetUsersWithTimeStamps(DateTime earliestTimeStamp)
        {
            return string.Format("SELECT username, user_cg, user_store_sn, mid FROM tbl_users WHERE time_stamp >= '{0}';", 
                                 earliestTimeStamp.ToString("s"));
        }

        /// <summary>
        /// Gets all the stores from the store table
        /// </summary>
        /// <returns></returns>
        public string GetStores()
        {
            return "SELECT cg, serial_number, mid FROM tbl_store WHERE mid > 100;";
        }
    }
}
