require "spec_helper"

describe Octopus::Relation do
  describe "shard tracking" do
    before :each do
      @client = Client.using(:canada).create!
      @client.items << Item.using(:canada).create!
      @relation = @client.items
    end

    it "remembers the shard on which a relation was created" do
      @relation.current_shard.should eq(:canada)
    end

    it "is equal to an identically-defined, but different, RelationProxy" do
      i = @client.items
      @relation.should eq(i)
      @relation.object_id.should eq(i.object_id)
    end

    context "when comparing to other Relation objects" do
      before :each do
        @relation.reset
      end

      it "is equal to its clone" do
        @relation.should eq(@relation.clone)
      end
    end

    context "when no explicit shard context is provided" do
      it "uses the correct shard" do
        @relation.count.should eq(1)
      end

      it "lazily evaluates on the correct shard" do
        # Do something to force Client.connection_proxy.current_shard to change
        other_count = Client.using(:brazil).count
        @relation.select(:client_id).count.should == 1
      end
    end

    context "when an explicit, but different, shard context is provided" do
      it "uses the correct shard" do
        Item.using(:brazil).count.should eq(0)
        clients_on_brazil = Client.using(:brazil).all
        Client.using(:brazil) do
          @relation.count.should eq(1)
        end
      end

      it "lazily evaluates on the correct shard" do
        Item.using(:brazil).count.should eq(0)
        Client.using(:brazil) do
          @relation.select(:client_id).count.should == 1
        end
      end
    end

    context "when relation is passed as argument" do
      it "does not fail" do
        @client.items.where(id: @relation).count.should eq(1)
      end
    end
  end
end
