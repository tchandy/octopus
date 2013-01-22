require "spec_helper"

describe Octopus::Migration do
  it "should run just in the master shard" do
    migrating_to_version 1 do
      User.using(:master).find_by_name("Master").should_not be_nil
      User.using(:canada).find_by_name("Master").should be_nil
    end
  end

  it "should run on specific shard" do
    migrating_to_version 2 do
      User.using(:master).find_by_name("Sharding").should be_nil
      User.using(:canada).find_by_name("Sharding").should_not be_nil
    end
  end

  it "should run on specifieds shards" do
    migrating_to_version 3 do
      User.using(:brazil).find_by_name("Both").should_not be_nil
      User.using(:canada).find_by_name("Both").should_not be_nil
    end
  end

  it "should run on specified group" do
    migrating_to_version 4 do
      User.using(:canada).find_by_name("Group").should_not be_nil
      User.using(:brazil).find_by_name("Group").should_not be_nil
      User.using(:russia).find_by_name("Group").should_not be_nil
    end
  end

  it "should run once per shard" do
    migrating_to_version 5 do
      User.using(:canada).find(:all, {:conditions => {:name => "MultipleGroup"}}).size.should == 1
      User.using(:brazil).find(:all, {:conditions => {:name => "MultipleGroup"}}).size.should == 1
      User.using(:russia).find(:all, {:conditions => {:name => "MultipleGroup"}}).size.should == 1
    end
  end

  it "should create users inside block" do
    migrating_to_version 12 do
      User.using(:brazil).find(:all, :conditions => {:name => "UsingBlock1"}).size.should == 1
      User.using(:brazil).find(:all, :conditions => {:name => "UsingBlock2"}).size.should == 1
      User.using(:canada).find(:all, :conditions => {:name => "UsingCanada"}).size.should == 1
      User.using(:canada).find(:all, :conditions => {:name => "UsingCanada2"}).size.should == 1
    end
  end

  it "should send the query to the correct shard" do
    migrating_to_version 13 do
      User.using(:brazil).find(:all, :conditions => {:name => "Brazil"}).size.should == 1
      User.using(:brazil).find(:all, :conditions => {:name => "Canada"}).size.should == 0
      User.using(:canada).find(:all, :conditions => {:name => "Brazil"}).size.should == 0
      User.using(:canada).find(:all, :conditions => {:name => "Canada"}).size.should == 1
    end
  end

  describe "when using replication" do
    it "should run writes on master when you use replication" do
      using_environment :production_replicated do
        migrating_to_version 10 do
          Cat.find_by_name("Replication").should be_nil
        end
      end
    end

    it "should run in all shards, master or another shards" do
      using_environment :production_replicated do
        migrating_to_version 11 do
          [:slave4, :slave1, :slave2, :slave3].each do |sym|
            Cat.find_by_name("Slaves").should_not be_nil
          end
        end
      end
    end
  end

  it "should store the migration versions in each shard" do
    class SchemaMigration < ActiveRecord::Base; end

    migrating_to_version 14 do
      Octopus.using(:canada) { ActiveRecord::Migrator.get_all_versions }.should include(14)
      Octopus.using(:brazil) { ActiveRecord::Migrator.get_all_versions }.should include(14)
      Octopus.using(:russia) { ActiveRecord::Migrator.get_all_versions }.should include(14)
    end
  end

  it "should run the migrations on shards that are missing them" do
    class SchemaMigration < ActiveRecord::Base; end

    Octopus.using(:master) { SchemaMigration.create(:version => 14) }
    Octopus.using(:canada) { SchemaMigration.create(:version => 14) }

    migrating_to_version 14 do
      Octopus.using(:canada) { ActiveRecord::Migrator.get_all_versions }.should include(14)
      Octopus.using(:brazil) { ActiveRecord::Migrator.get_all_versions }.should include(14)
      Octopus.using(:russia) { ActiveRecord::Migrator.get_all_versions }.should include(14)
    end
  end
end
