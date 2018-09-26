using System.Collections.Generic;

namespace MoveUsersTo_lic_storeuser
{
    public class User
    {
        private const long UnknownUserMid = 3;

        /// <summary>
        /// The user's name
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// The list of serial numbers this user has access to as a pipe-delimited string
        /// </summary>
        public List<int> StoreSNList { get; set; }

        /// <summary>
        /// List of consolidation groups this user can view
        /// </summary>
        public List<int> CGList { get; set; }

        /// <summary>
        /// The user's mid
        /// </summary>
        public long Mid { get; set; }

        /// <summary>
        /// A list of store mids for the stores listed in UserStoreSN
        /// </summary>
        public List<long> StoreMidList { get; }    

        public User()
        {
            this.StoreMidList = new List<long>();
            this.UserName     = string.Empty;
            this.StoreSNList  = new List<int>();
            this.CGList       = new List<int>();
            this.Mid          = User.UnknownUserMid;
        }
    }
}
