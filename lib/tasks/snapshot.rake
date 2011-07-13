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
    data = []

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
    File.open(to, "w") { |out| YAML.dump(data, out) }
  end

  namespace :snapshot do
    desc <<DESC
Restores the contents of the database (defaults to db/snapshot). If
the schema does not match, the database is first reset to the
snapshot's schema, and then migrations are run to bring the snapshot
up to date. The snapshot is then retaken with the updated schema.

  rake db:snapshot:restore
  rake db:snapshot:restore[db/scenarios/fully-loaded]
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
      data.each do |table|
        table_name = table[:table]
        next if table_name == "schema_migrations"

        STDERR.puts "- #{table_name} (#{table[:rows].length} rows)..."

        columns = conn.columns(table_name)
        fields = columns.map { |c| conn.quote_column_name(c.name) }.join(",")
        pfx = "INSERT INTO #{conn.quote_table_name(table_name)} (#{fields}) VALUES ("
        sfx = ")"

        table[:rows].each do |row|
          sql = pfx + columns.map { |c| conn.quote(row[c.name], c) }.join(",") + sfx
          conn.insert_sql(sql)
        end
      end

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
