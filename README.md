gateplugin-virtualcorpus
========================

A plugin for the GATE language technology framework that provides corpus LRs for directories and JDBC databases

Current Features
----------------
* jdbc corpus (including libraries for sqlite, postgres, mysql/mariadb, h2)
* directory corpus with recursive support 
* read-only documents: changes in gate are not persisted 

Comparison to johann-petrak/gateplugin-VirtualCorpus
----------------------------------------------------
* No dummy datastore support/obligation --> corpus & documents can be stored in regular datastores (e.g. serial datastore)
* normal gate persistence --> corpus with its initialization parameters can be saved within an application file (.xgapp), documents will be reloaded from corpus
* refactored and cleaned up code