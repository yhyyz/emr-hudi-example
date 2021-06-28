if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: run")
end

sysbench.cmdline.options = {
	skip_trx = {"Do not use BEGIN/COMMIT; Use global auto_commit value", false}
}

create_tb_1 = [[ CREATE TABLE if not exists `test_tb_01` (
                    `id` int NOT NULL AUTO_INCREMENT,
                    `name` varchar(155) DEFAULT NULL,
                    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `modify_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`)
                  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
             ]]
create_tb_2 = [[ CREATE TABLE if not exists `test_tb_02` (
                    `id` int NOT NULL AUTO_INCREMENT,
                    `name` varchar(155) DEFAULT NULL,
                    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `modify_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`)
                  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
             ]]

local create_test_tb = {
	create_tb_1,
	create_tb_2,
}


function execute_creates()
    con:query(string.format(create_test_tb[1]))
    con:query(string.format(create_test_tb[2]))
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

	execute_creates()

	if not sysbench.opt.skip_trx then
		con:query("COMMIT")
	end
end