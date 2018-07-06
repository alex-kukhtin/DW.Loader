using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace DW.Loader
{
    public enum JobType
    {
        Start,
        Journal,
        Catalog,
        Document
    };
    public class JobSettings : ConfigurationElement
    {
        const String cType = "type";
        const String cDataKey = "dataKey";
        const String cUseGuid = "useGuid";
        const String cTargetProcedurePrefix = "targetProcedurePrefix";
        const String cSourceProcedurePrefix = "sourceProcedurePrefix";

        [ConfigurationProperty("name", IsKey=true, IsRequired=true)]
        public string Name
        {
            get
            {
                return (String) this["name"];
            }
            set
            {
                this["name"] = value;
            }
        }
        internal String Key { get { return Name; } }
        [ConfigurationProperty("source", IsRequired = true)]
        public String Source
        {
            get
            {
                return (String)this["source"];
            }
            set
            {
                this["source"] = value;
            }
        }
        [ConfigurationProperty("target", IsRequired = true)]
        public String Target
        {
            get
            {
                return (String)this["target"];
            }
            set
            {
                this["target"] = value;
            }
        }
        [ConfigurationProperty(cSourceProcedurePrefix, IsRequired = true)]
        public String SourceProcedurePrefix
        {
            get
            {
                return (String)this[cSourceProcedurePrefix];
            }
            set
            {
                this[cSourceProcedurePrefix] = value;
            }
        }
        [ConfigurationProperty(cTargetProcedurePrefix, IsRequired = true)]
        public String TargetProcedurePrefix
        {
            get
            {
                return (String)this[cTargetProcedurePrefix];
            }
            set
            {
                this[cTargetProcedurePrefix] = value;
            }
        }
        [ConfigurationProperty(cType, IsRequired = true)]
        public JobType Type
        {
            get
            {
                return (JobType)this[cType];
            }
            set
            {
                this[cType] = value;
            }
        }
        [ConfigurationProperty(cDataKey, IsRequired = true)]
        public String DataKey
        {
            get
            {
                return (String)this[cDataKey];
            }
            set
            {
                this[cDataKey] = value;
            }
        }
        [ConfigurationProperty(cUseGuid, IsRequired = false)]
        public Boolean UseGuid
        {
            get
            {
                return (Boolean)this[cUseGuid];
            }
            set
            {
                this[cDataKey] = value;
            }
        }
    }

    public class JobsCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new JobSettings();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((JobSettings)element).Name;
        }
        public override ConfigurationElementCollectionType CollectionType
        {
            get
            {
                return ConfigurationElementCollectionType.AddRemoveClearMap;
            }
        }
    }

    public class Jobs : ConfigurationSection
    {
        [ConfigurationProperty("", IsDefaultCollection=true)]
        public JobsCollection jobs
        {
            get {
                return (JobsCollection)base[""];
            }
        }
    }
}
