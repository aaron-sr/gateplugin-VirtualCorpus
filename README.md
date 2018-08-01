gateplugin-virtualcorpus
========================

A plugin for the GATE language technology framework that provides corpus LRs for directories and JDBC databases

Current Features
----------------
* jdbc corpus (including libraries for sqlite, postgres, mysql/mariadb, h2)
* directory corpus with recursive support
* encoding and mimeType support to read and write content from/to backend

Comparison to johann-petrak/gateplugin-VirtualCorpus (06/2017)
----------------------------------------------------
* Support of new Gate 8.5 plugin architecture based on maven
* No dummy datastore support/obligation --> corpus & documents can be stored in regular datastores (e.g. serial datastore)
* normal gate persistence --> corpus with its initialization parameters can be saved within an application file (.xgapp), documents will be reloaded from corpus and not stored in application file
* refactored and cleaned up code, easy to implement new virtual corpus