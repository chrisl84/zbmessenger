# zbmessenger

Script that reads a Bacula formatted email via stdin, extracts the information relevant to the Bacula job and then sends the data to a Zabbix Server.

Requires a Bacula template to exist on the Zabbix server.

## Parameters :

--verbose / -v : Logs debug messages.

--quiet / -q : Logs warning messages only.

--zabbix_binaries : Zabbix_sender utility location

--zabbix_server : IP/Hostname to Zabbix Server

--logfile : Location of log file, defaults to /var/log/zbmessenger.log

--debug_send : Send each key/value individually to the zabbix server.

## Requirements :
* [Python 3.6](https://www.python.org/).
* [Zabbix Sender](http://manpages.ubuntu.com/manpages/bionic/man1/zabbix_sender.1.html)

## Tested on:
* Linux/Ubuntu.

## GPLv3 License
* [GPLv3](http://www.gnu.org/licenses/).

