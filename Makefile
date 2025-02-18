
compile:
	cd /root/bf-sde-9.4.0/ ; sh . ../tools/./set_sde.bash
	~/tools/p4_build.sh --with-p4c=bf-p4c /home/Amazloum/P4_DMZ_realDeployment/p4src/Per_Flow_Collector.p4

run:
	pkill switchd 2> /dev/null ; cd /root/bf-sde-9.4.0/ ;./run_switchd.sh -p Per_Flow_Collector

conf_links:
	cd /root/bf-sde-9.4.0/ ; ./run_bfshell.sh --no-status-srv -f /home/Amazloum/P4_DMZ_realDeployment/ucli_cmds

start_control_plane_measurements:
	/root/bf-sde-9.4.0/./run_bfshell.sh --no-status-srv -i -b /home/Amazloum/P4_DMZ_realDeployment/bfrt_python/control_plane.py

