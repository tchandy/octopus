appraise "rails42" do
  gem "activerecord", "~> 4.2.0"
end

appraise "rails42_db2" do
  gem "activerecord", "~> 4.2.0"
  #Causes issue https://github.com/ibmdb/ruby-ibmdb/issues/31
  #  Wait for this PR to be accepted or pull in a local gem for yourself:
  #  https://github.com/ibmdb/ruby-ibmdb/pull/38/files
  gem "ibm_db", "~> 3.0.5"
end

appraise "rails50_db2" do
  gem "activerecord", "~> 5.0.0"
  # For now, ibm_db only works on rails 5.0
  #gem "ibm_db", "~> 4.0.0"
  # 4.0.0 breaks on empty_insert_statement_value issue:
  # https://github.com/ibmdb/ruby-ibmdb/pull/89
  # I've merged this into my own branch for testing until
  # IBM fixes this issue officially
  gem "ibm_db", 
    git: "https://github.com/calh/ruby-ibmdb.git", 
    branch: "v4.0.1",
    glob: "IBM_DB_Adapter/ibm_db/IBM_DB.gemspec"
end

appraise "rails50" do
  gem "activerecord", "~> 5.0.0"
end

appraise "rails51" do
  gem "activerecord", "~> 5.1.0"
end

appraise "rails52" do
  gem "activerecord", "~> 5.2.0"
end
# vim: ft=ruby
