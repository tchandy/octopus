require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe Octopus::Logger do

	before :each do
		@out = StringIO.new
		@log = Octopus::Logger.new(@out)
		ActiveRecord::Base.logger = @log
	end

	after :each do
		ActiveRecord::Base.logger = nil
  end

	it "should add to the default logger what shard the query was sent" do
    User.using(:canada).create!(:name => "test")
		@out.string.should =~ /Shard: canada/
	end
end