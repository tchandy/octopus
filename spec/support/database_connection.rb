ActiveRecord::Base.establish_connection(:adapter => "mysql", :database => "octopus_shard_1", :username => "root", :password => "")
ActiveRecord::Base.logger = Logger.new(File.open('database.log', 'a'))
