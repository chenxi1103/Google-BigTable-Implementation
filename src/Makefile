include hosts.mk

# EDIT THIS
MASTER_CMD=python3 master_server.py
TABLET_CMD=python3 tablet_server.py
# END EDIT REGION

# if you require any compilation, fill in this section
compile:
	echo "no compile"

grade1:
	cd grading/ && python3 grading.py 1 $(TABLET_HOSTNAME) $(TABLET_PORT)

grade2:
	python3 grading/grading.py 2 $(MASTER_HOSTNAME) $(MASTER_PORT)

master:
	$(MASTER_CMD) $(MASTER_HOSTNAME) $(MASTER_PORT)

tablet1:
	$(TABLET_CMD) $(TABLET1_HOSTNAME) $(TABLET1_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)

tablet2:
	$(TABLET_CMD) $(TABLET2_HOSTNAME) $(TABLET2_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)

tablet3:
	$(TABLET_CMD) $(TABLET3_HOSTNAME) $(TABLET3_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)

.PHONY: master tablet1 tablet2 tablet3 grade1 grade2 compile

