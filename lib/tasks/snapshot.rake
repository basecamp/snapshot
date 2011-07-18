class SnapshotHelper
  def initialize
    @on_snapshot = []
    @on_restore  = []
  end

  def on_snapshot(&block)
    @on_snapshot.push block
  end

  def on_restore(&block)
    @on_restore.push block
  end

  def snapshot!(path)
    @on_snapshot.each do |callback|
      callback.call(path)
    end
  end

  def restore!(path)
    @on_restore.each do |callback|
      callback.call(path)
    end
  end
end

DBSnapshot = SnapshotHelper.new

namespace :db do
  desc <<DESC
Takes a snapshot of the current database (defaults to db/snapshot).
whatever parameter is given.

  rake db:snapshot
  rake db:snapshot[db/scenarios/fully-loaded]
DESC
  task :snapshot, [:to] => "db:schema:dump" do |t, args|
    to = args[:to] || "db/snapshot"

    cp "db/schema.rb", to + ".schema"
    conn = ActiveRecord::Base.connection

    meta = {
      :now => Time.now.utc,
      :tables => (data = []) }

    conn.tables.each do |table|
      next if table == "schema_migrations"

      size = conn.select_value("SELECT count(*) FROM #{conn.quote_table_name(table)}").to_i
      next if size.zero?

      STDERR.puts "- #{table} (#{size} rows)..."
      data << {
        :table => table,
        :rows => conn.select_all("SELECT * FROM #{conn.quote_table_name(table)}")
      }
    end

    STDERR.puts "writing snapshot to #{to}..."
    File.open(to, "w") { |out| YAML.dump(meta, out) }

    DBSnapshot.snapshot!(to)
  end

  namespace :snapshot do
    desc <<DESC
Restores the contents of the database (defaults to db/snapshot). If
the schema does not match, the database is first reset to the
snapshot's schema, and then migrations are run to bring the snapshot
up to date. The snapshot is then retaken with the updated schema.

  rake db:snapshot:restore
  rake db:snapshot:restore[db/scenarios/fully-loaded]

Times and dates will be computed relative to the current time, unless
you specify the NOW variable instead.

  rake db:snapshot:restore NOW='2011-01-01T12:00:00Z'
DESC
    task :restore, [:from] => :environment do |t, args|
      abort "refusing to restore snapshot in production" if Rails.env.production?

      from = args[:from] || "db/snapshot"
      abort "snapshot file does not exist (#{from})" unless File.exists?(from)

      STDOUT.sync = true # we want to emit output as we generate it, immediately
      conn = ActiveRecord::Base.connection

      # If the database is not empty, prompt for confirmation before destroying the
      # existing data.
      conn.tables.each do |table|
        next if table == "schema_migrations"
        if conn.select_value("SELECT count(*) FROM #{conn.quote_table_name(table)}").to_i > 0
          puts "This database is not empty. Are you sure you want to erase it and load the snapshot? [y/n]"
          answer = STDIN.gets || ""
          abort "aborting without restoring the snapshot" unless answer.strip == "y"
          break
        end
      end

      # load the original schema
      load("#{from}.schema")

      # load the data
      data = YAML.load_file(from)
      relative_to = Time.parse(data[:now].to_s)

      now = (ENV['NOW'] && Time.parse(ENV['NOW'])) || Time.now.utc
      today = now.to_date

      data[:tables].each do |table|
        table_name = table[:table]

        STDERR.puts "- #{table_name} (#{table[:rows].length} rows)..."

        columns = conn.columns(table_name)
        fields = columns.map { |c| conn.quote_column_name(c.name) }.join(",")
        pfx = "INSERT INTO #{conn.quote_table_name(table_name)} (#{fields}) VALUES ("
        sfx = ")"

        table[:rows].each do |row|
          values = columns.map do |c|
            value = case c.type
              when :datetime then
                now - (relative_to - Time.parse(row[c.name].to_s)) if row[c.name]
              when :date
                today - (relative_to.to_date - Date.parse(row[c.name].to_s)) if row[c.name]
              else
                row[c.name]
              end
            conn.quote(value, c)
          end

          conn.insert_sql(pfx + values.join(",") + sfx)
        end
      end

      DBSnapshot.restore!(from)

      # make sure we're all up-to-date, schema-wise
      migrator = ActiveRecord::Migrator.new(:up, "db/migrate/")

      if migrator.pending_migrations.any?
        STDERR.puts "catching up on migrations..."
        migrator.migrate
        Rake::Task["db:snapshot"].invoke
      end
    end
  end
end
