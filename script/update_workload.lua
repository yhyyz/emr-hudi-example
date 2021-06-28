if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: run")
end

sysbench.cmdline.options = {
	skip_trx = {"Do not use BEGIN/COMMIT; Use global auto_commit value", false},
	update_id_min = {"update table rand id max", 1},
	update_id_max = {"update table rand id max", 50000}
}


local updates = {
	"update cdc_test_db.test_tb_01 set name ='%s' where id = %d",
	"update cdc_test_db.test_tb_02 set name ='%s' where id = %d",
}


function create_random_name()
	local username = sysbench.rand.string(string.rep("@",sysbench.rand.uniform(5,10)))
	return username
end


function execute_updates()

	-- generate fake name
	local name = create_random_name()
    con:query(string.format(updates[1], name, sysbench.rand.special(sysbench.opt.update_id_min, sysbench.opt.update_id_max)))
	-- generate fake name
	local name = create_random_name()
    con:query(string.format(updates[2], name, sysbench.rand.special(sysbench.opt.update_id_min, sysbench.opt.update_id_max)))

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

	execute_updates()

	if not sysbench.opt.skip_trx then
		con:query("COMMIT")
	end
end