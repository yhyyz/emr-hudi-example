if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: run")
end

sysbench.cmdline.options = {
	skip_trx = {"Do not use BEGIN/COMMIT; Use global auto_commit value", false}
}

local page_types = { "actor", "character", "movie" }

local inserts = {
	"INSERT INTO cdc_test_db.test_tb_01 (name) VALUES ('%s')",
	"INSERT INTO cdc_test_db.test_tb_02 (name) VALUES ('%s')",
}


function create_random_name()
	local username = sysbench.rand.string(string.rep("@",sysbench.rand.uniform(5,10)))
	return username
end


function execute_inserts()

	-- generate fake name
	local name = create_random_name()

	-- INSERT for cdc_test_db.test_db_01
	con:query(string.format(inserts[1], name))
    -- INSERT for cdc_test_db.test_db_01
    con:query(string.format(inserts[2], name))

end


-- Called by sysbench to initialize script
function thread_init()

	-- globals for script
	drv = sysbench.sql.driver()
	con = drv:connect()
end


-- Called by sysbench when tests are done
function thread_done()

	con:disconnect()
end


-- Called by sysbench for each execution
function event()

	if not sysbench.opt.skip_trx then
		con:query("BEGIN")
	end

	execute_inserts()

	if not sysbench.opt.skip_trx then
		con:query("COMMIT")
	end
end