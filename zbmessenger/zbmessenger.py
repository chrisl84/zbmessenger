#!/usr/bin/python3


from argparse import ArgumentParser
from datetime import datetime
import subprocess
import logging
import logging.handlers
import operator
import sys
import re


class BaculaEmailParser:

    def __init__(self):
        """
        Parses the information from the bacula job using regular expression.
        """
        self._logger = logging.getLogger('zbmessenger.BaculaEmailParser')
        # Default content applicable to all bacula information lines.
        self._defaults = ['^[^\S\n]*', '[^\S\n]*', '$']
        # List of all the regular expressions for each line in bacula job report
        self._bacula = {
            "job_id": '{0}JobId:{1}(\d+){2}'.format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "job": "{0}Job:{1}(.[^\.]+)\.(.[^_]+)\_(.[^\_]+)\_(.+){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "backup_level": "{0}Backup Level:{1}(.[^,]+)(?:, since=)?(\d\d\d\d-\d\d-\d\d)?[ ]?(\d\d:\d\d:\d\d)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "client": "{0}Client:{1}\"(.+)\"[ ]?(.+)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "file_set": "{0}FileSet:{1}\"(.+)\"[ ]?(\d\d\d\d-\d\d-\d\d)?[ ]?(\d\d:\d\d:\d\d)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "pool": "{0}Pool:{1}\"(.+)\"[ ]?(.+)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "catalog": "{0}Catalog:{1}\"(.+)\"[ ]?(.+)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "storage": "{0}Storage:{1}\"(.+)\"[ ]?(.+)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "scheduled_time": "{0}Scheduled time:{1}(\d\d-[a-zA-Z]{{3,4}}-\d\d\d\d \d\d:\d\d:\d\d){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "start_time": "{0}Start time:{1}(\d\d-[a-zA-Z]{{3,4}}-\d\d\d\d \d\d:\d\d:\d\d){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "end_time": "{0}End time:{1}(\d\d-[a-zA-Z]{{3,4}}-\d\d\d\d \d\d:\d\d:\d\d){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "elapsed_time": "{0}Elapsed time:{1}(\d*)[ ]?([a-zA-Z]*)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "priority": "{0}Priority:{1}([-]?\d*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "fd_files_written": "{0}FD Files Written:{1}([0-9,]*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "sd_files_written": "{0}SD Files Written:{1}([0-9,]*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "fd_bytes_written": "{0}FD Bytes Written:{1}([\d,]*).*{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "sd_bytes_written": "{0}SD Bytes Written:{1}([\d,]*).*{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "rate": "{0}Rate:{1}([0-9.,]*)[ ]?(.*)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "sw_compression": "{0}Software Compression:{1}([\d]*[\.[\d]*]?)?(?:[\%])?(?:[ ])?(?:([\d.]*)\:([\d]*))?(None)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "cl_compression": "{0}Comm Line Compression:{1}([\d]*[\.[\d]*]?)?(?:[\%])?(?:[ ])?(?:([\d.]*)\:([\d]*))?(None)?{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "snapshot": "{0}Snapshot/VSS:{1}(no|yes){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "encryption": "{0}Encryption:{1}(no|yes){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "accurate": "{0}Accurate:{1}(no|yes){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "volume_name": "{0}Volume name\(s\):{1}(.*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "volume_session_id": "{0}Volume Session Id:{1}(\d*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "volume_session_time": "{0}Volume Session Time:{1}(\d*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "lvbytes": "{0}Last Volume Bytes:{1}([0-9,]*).*{2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "fd_errors": "{0}Non-fatal FD errors:{1}(\d*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "sd_errors": "{0}SD Errors:{1}(\d*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "fd_term": "{0}FD termination status:{1}(.*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "sd_term": "{0}SD termination status:{1}(.*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            ),
            "termination": "{0}Termination:{1}(.*){2}".format(
                self._defaults[0],
                self._defaults[1],
                self._defaults[2]
            )
        }

    def parse_email(self, content):
        """
        Parses the email, extracts the relevant data that will be forward to the
        Zabbix Server
        """
        self._logger.info(content)
        parameters = {}
        for key, value in self._bacula.items():
            items = list()
            for y in re.findall(value, content, re.MULTILINE):
                # Instead of lists of tuples, flatten the list.
                # Discard any empty strings
                if isinstance(y, tuple):
                    for z in y:
                        if z is not '' and z.isspace() is False:
                            items.append(z)
                else:
                    if y is not '' and y.isspace() is False:
                        items.append(y)
            if len(items) == 0:
                items = None
            parameters[key] = items
            self._logger.info("Key:%s - Value : %s", key, value)
        return parameters


class ZabbixSender:
    # Uses SubPorcess to generate a shell and send parsed Bacula data to Zabbix
    def __init__(self, zabbix_sender_binaries):
        """
        Responsible for taking the data parsed by Baculaemail_parser and
        send it to the Zabbix server.
        When sending data to the zabbix server, I'm piping the content in to 
        the zabbix_sender binary as opposed to save the content to a file and then
        supply the file location.
        """
        self._logger = logging.getLogger('zbmessenger.ZabbixSender')
        self._binaries = zabbix_sender_binaries
        pass

    def send(self, zabbix_server, client, values, debug_send=False):
        # -z is the ip/hostname for the zabbix server
        # -s is the name of the host as specified in Zabbix (i.e the server that you want the values connected to)
        # -i and - tells zabbix_sender to wait for values to be piped in.
        command = [self._binaries,
                   '-z',
                   zabbix_server,
                   '-s',
                   client,
                   '-i',
                   '-'
                   ]
        # Used for debugging purposes.
        # Easier to spot which value Zabbix failed
        if debug_send:
            return_code = True
            for line in values.split('\n'):
                self._logger.debug("Executing command %s < %s", command, line)
                return_code &= self._execute(command, line)
            return return_code
        else:
            return self._execute(command, values)

    def _execute(self, command, values):
        """
        Launches a process and executes the command
        """
        process = subprocess.Popen(command,
                                   stdout=subprocess.PIPE,
                                   stdin=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        # Pipe in the values in binary format.
        stdout = process.communicate(input=values.encode())
        self._logger.debug(stdout)
        return_code = process.returncode
        if return_code != 0:
            self._logger.warning(stdout)
        return return_code == 0


class ZabbixParameters:
    def __init__(self):
        self._logger = logging.getLogger('zbmessenger.ZabbixParameters')
        # Converts time to seconds.
        self._time_specification = {
            'secs': 1,
            'mins': 60,
            'hrs': 3600,
            'days': 86400
        }
        # Converts transfer rates to KB/s
        self._transfer_specification = {
            'KB/s': 1,
            'MB/s': 1000,
            'GB/s': 1000000,
            'TB/s': 1000000000
        }
        # map key to the corresponding method
        self._converters = {
            "job_id": self.job_id,
            "job": self.job,
            "backup_level": self.backup_level,
            "client": self.client,
            "file_set": self.file_set,
            "pool": self.pool,
            "catalog": self.catalog,
            "storage": self.storage,
            "scheduled_time": self.scheduled_time,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "elapsed_time": self.elapsed_time,
            "priority": self.priority,
            "fd_files_written": self.fd_files_written,
            "sd_files_written": self.sd_files_written,
            "fd_bytes_written": self.fd_bytes_written,
            "sd_bytes_written": self.sd_bytes_written,
            "rate": self.rate,
            "sw_compression": self.sw_compression,
            "cl_compression": self.cl_compression,
            "snapshot": self.snapshot,
            "encryption": self.encryption,
            "accurate": self.accurate,
            "volume_name": self.volume_name,
            "volume_session_id": self.volume_session_id,
            "volume_session_time": self.volume_session_time,
            "lvbytes": self.lv_bytes,
            "fd_errors": self.fd_errors,
            "sd_errors": self.sd_errors,
            "fd_term": self.fd_term,
            "sd_term": self.sd_term,
            "termination": self.termination
        }
        self.values = {}
    # All methods below, takes the values from the regular exception and
    # and returns a value that is then sent to the zabbix sender.
    # Note that Boolean values are converted to ints (0 and 1).

    def job_id(self, value):
        try:
            return int(value[0]) if isinstance(value, list) else None
        except Exception as error:
            self._logger.warning(
                "Job id is not specified correctly %s.", value)
            return None

    def job(self, value):
        try:
            return value[0] if isinstance(value, list) else None
        except Exception as error:
            self._logger.warning(
                "Job is not specified correctly %s.", value)
            return None

    def backup_level(self, value):
        try:
            return value[0] if isinstance(value, list) else None
        except Exception as error:
            self._logger.warning(
                "Backup Level is not specified correctly %s.", value)
            return None

    def client(self, value):
        try:
            return value[0] if isinstance(value, list) else None
        except Exception as error:
            self._logger.warning(
                "Client is not specified correctly %s.", value)
            return None

    def file_set(self, value):
        try:
            return value[0] if isinstance(value, list) else None
        except Exception as error:
            self._logger.warning(
                "File Set is not specified correctly %s.", value)
            return None

    def pool(self, value):
        try:
            return value[0] if isinstance(value, list) else None
        except Exception as error:
            self._logger.warning("Pool is not specified correctly %s.", value)
            return None

    def catalog(self, value):
        try:
            return value[0] if isinstance(value, list) else None
        except Exception as error:
            self._logger.warning(
                "Catalog is not specified correctly %s.", value)
            return None

    def storage(self, value):
        try:
            return value[0]
        except Exception as error:
            self._logger.warning(
                "Storage is not specified correctly %s.", value)
            return None

    def scheduled_time(self, value):
        try:
            return datetime.strptime(value[0], '%d-%b-%Y %H:%M:%S')
        except Exception as error:
            self._logger.warning(
                "Scheduled Time is not specified correctly %s.", value)
            return None

    def start_time(self, value):
        try:
            return datetime.strptime(value[0], '%d-%b-%Y %H:%M:%S')
        except Exception as error:
            self._logger.warning(
                "Start Time is not specified correctly %s.", value)
            return None

    def end_time(self, value):
        try:
            return datetime.strptime(value[0], '%d-%b-%Y %H:%M:%S')
        except Exception as error:
            self._logger.warning(
                "End Time is not specified correctly %s.", value)
            return None

    def elapsed_time(self, value):
        try:
            if value is None or len(value) == 0:
                return None
            elapsed_time = 0
            it = iter(value)
            for item in it:
                time = int(item)
                spec = self._time_specification.get(next(it), -1)
                if spec == -1:
                    self._logger.warning("Unknown time specification for elapsed time : %s",
                                         value)
                    return None
                else:
                    elapsed_time += time * spec
            return elapsed_time
        except Exception as error:
            self._logger.warning(
                "Elapsed time is not specified correctly %s.", value)
            return None

    def priority(self, value):
        try:
            return int(value[0])
        except Exception as error:
            self._logger.warning(
                "Priority is not specified correctly %s.", value)

    def fd_files_written(self, value):
        try:
            return int(value[0].replace(',', ''))
        except Exception as error:
            self._logger.warning(
                "FD Files Written is not specified correctly %s.", value)
            return None

    def sd_files_written(self, value):
        try:
            return int(value[0].replace(',', ''))
        except Exception as error:
            self._logger.warning(
                "SD Files Written is not specified correctly %s.", value)
            return None

    def fd_bytes_written(self, value):
        try:
            return int(value[0].replace(',', ''))
        except:
            self._logger.warning(
                "FD Bytes Written is not specified correctly %s.", value)
            return None

    def sd_bytes_written(self, value):
        try:
            return int(value[0].replace(',', ''))
        except Exception as e:
            self._logger.warning(
                "SD Bytes Written is not specified correctly %s.", value)
            return None

    def rate(self, value):
        try:
            rate = float(value[0].replace(',', ''))
            spec = self._transfer_specification.get(value[1], -1)
            if spec == -1:
                self._logger.warning(
                    "Unknown transfer specification %s for rate.", value)
                return None
            else:
                return rate * spec
        except Exception as e:
            self._logger.warning(
                "Rate is not specified correctly %s.", value)
            return None

    def sw_compression(self, value):
        try:
            comp = value[0]
            if comp != 'None':
                return float(comp)
            else:
                return None
        except Exception as error:
            self._logger.warning(
                "SW Compression is not specified correctly %s.", value)
            return None

    def cl_compression(self, value):
        try:
            comp = value[0]
            if(comp != 'None'):
                return float(comp)
            else:
                return None
        except Exception as error:
            self._logger.warning(
                "CL Compression is not specified correctly %s.", value)

    def snapshot(self, value):
        try:
            snap = value[0]
            if snap == 'yes':
                return 1
            elif snap == 'no':
                return 0
            else:
                self._logger.warning(
                    "Unknown snapshot value %s.", snap)
                return None
        except Exception as error:
            self._logger.warning(
                "Snapshot is not specified correctly %s.", value)
            return None

    def encryption(self, value):
        try:
            enc = value[0]
            if enc == 'yes':
                return 1
            elif enc == 'no':
                return 0
            else:
                self._logger.warning(
                    "Unknown encryption value %s.", enc)
                return None
        except Exception as error:
            self._logger.warning(
                "Encryption is not specified correctly %s.", value)
            return None

    def accurate(self, value):
        try:
            ac = value[0]
            if ac == 'yes':
                return 1
            elif ac == 'no':
                return 0
            else:
                self._logger.warning(
                    "Unknown accurate value %s.", ac)
                return None
        except Exception as error:
            self._logger.warning(
                "Accurate is not specified correctly %s.", value)
            return None

    def volume_name(self, value):
        try:
            return value[0] if isinstance(value, list) else None
        except Exception as error:
            self._logger.warning(
                "Volume Name is not specified correctly %s.", value)
            return None

    def volume_session_id(self, value):
        try:
            return int(value[0])
        except Exception as e:
            self._logger.warning(
                "Volume Session ID is not specified correctly %s.", value)
            return None

    def volume_session_time(self, value):
        try:
            return int(value[0])
        except Exception as e:
            self._logger.warning(
                "Volume Session Time is not specified correctly %s.", value)
            return None

    def lv_bytes(self, value):
        try:
            parsed = int(value[0].replace(',', ''))
            if parsed < 0:
                raise Exception('Value cannot be less than 0.')
            return parsed
        except Exception as e:
            self._logger.warning(
                "LV Bytes is not specified correctly %s.", value)
            return None

    def fd_errors(self, value):
        try:
            parsed = int(value[0])
            if parsed < 0:
                raise Exception("Number of FD errors cannot be less than 0.")
            return parsed
        except Exception as e:
            self._logger.warning(
                "FD Errors is not specified correctly %s.", value)
            return None

    def sd_errors(self, value):
        try:
            parsed = int(value[0])
            if parsed < 0:
                raise Exception("Number of SD errors cannot be less than 0.")
            return parsed
        except Exception as e:
            self._logger.warning(
                "SD Errors is not specified correctly %s.", value)
            return None

    def fd_term(self, value):
        try:
            if isinstance(value, list) is False:
                raise Exception('FD term is not a list.')
            if value[0] is '' or value[0].isspace():
                return None
            return 1 if value[0] is 'OK' else 0
        except Exception as e:
            self._logger.warning(
                "FD Termination is not specified correctly %s.", value)
            return None

    def sd_term(self, value):
        try:
            if isinstance(value, list) is False:
                raise Exception('SD term is not a list.')
            if value[0] is '' or value[0].isspace():
                return None
            return 1 if value[0] is 'OK' else 0
        except Exception as e:
            self._logger.warning(
                "SD Termination is not specified correctly %s.", value)
            return None

    def termination(self, value):
        try:
            if isinstance(value, list) is False:
                raise Exception('Termination is not a list.')
            if value[0] is '' or value[0].isspace():
                return None
            return 1 if 'Error' not in value[0] else 0
        except Exception as e:
            self._logger.warning(
                "Termination is not specified correctly %s.", value)
            return None

    def format(self, content):
        """
        Format each item and store it in a dictionary.
        Return dictionary
        """
        all_items = {}
        for key, value in content.items():
            try:
                method = self._converters.get(key)
                if method is not None:
                    all_items[key] = method(value)
                else:
                    self._logger.warning(
                        "Unable to find a method with the name %s.", key)
            except Exception as e:
                self._logger.exception("Formatting Exception")
        return all_items

    def get_client(self, items):
        """
        Helper method to get the client name.
        This is used when specifying the name of the Zabbix host
        the values should be associated with.
        """
        return items.get('client')

    def parameters(self, values):
        """
        When the information is piped to the zabbix_sender binary, it should have the format of:
        <zabbix host> <key> <value>
        <zabbix host> <key1> <value1>
        We'll format all the values here, each new value requires a new line, max number of values
        supported by zabbix_sender is 250, however, we don't reach that threshold now.
        * Note that if you supply the zabbix host name in the zabbix_sender command using the -s command, 
        * you can substitute <zabbix host> with the letter '-'.
        """
        return "\n".join(['%s bacula.%s %s' % ('-', key, value) for (key, value) in values.items() if key is not 'client'])


class Main:

    def _readMessage(self):
        """
        Reads the email content that is piped in by Postfix.
        """
        content = ''
        for line in sys.stdin:
            content += line
        return content

    def _setupLogging(self, logfile='/tmp/zbmessenger.log'):
        """
        Configures location and default log level.
        """
        self._logger = logging.getLogger('zbmessenger')
        self._logger.setLevel(logging.DEBUG)
        self._fh = logging.handlers.RotatingFileHandler(
            logfile,
            maxBytes=2000000,
            backupCount=5)
        self._fh.setLevel(logging.INFO)
        self._formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self._fh.setFormatter(self._formatter)
        self._logger.addHandler(self._fh)

    def main(self):
        try:
            cmd_parser = ArgumentParser()
            cmd_parser.add_argument(
                '--verbose',
                '-v',
                help="Verbose logging enabled.",
                action='store_true'
            )
            cmd_parser.add_argument(
                '--quiet',
                '-q',
                help="Log only warning messages.",
                action='store_true'
            )
            # Location of the zabbix_sender binary, required.
            cmd_parser.add_argument(
                '--zabbix_binaries',
                type=str,
                help='Location of the zabbix_sender executable.',
                default=None,
                required=True
            )
            # IP or hostname of the zabbix server, required.
            cmd_parser.add_argument(
                '--zabbix_server',
                type=str,
                help='IP/Hostname to Zabbix Server',
                default=None,
                required=True,
            )
            # Log file level, default to /var/log/zbmessenger.log
            cmd_parser.add_argument(
                '--logfile',
                type=str,
                help='Location of log file, defaults to /var/log/zbmessenger.log',
                default='/var/log/zbmessenger.log',
                required=False
            )
            cmd_parser.add_argument(
                '--debug_send',
                '-ds',
                help="Send each key/value individually to the zabbix server.",
                action='store_true'
            )

            try:
                cmds = vars(cmd_parser.parse_args())
                logfile = cmds.get('logfile')
                self._setupLogging(logfile)
                if cmds.get('verbose') is True:
                    self._fh.setLevel(logging.DEBUG)
                elif cmds.get('quiet') is True:
                    self._fh.setLevel(logging.WARN)
                # Read data from Pipe
                bacula_email = self._readMessage()
                email_parser = BaculaEmailParser()
                # parse the email content.
                parameters = email_parser.parse_email(bacula_email)
                self._logger.info(parameters)
                converter = ZabbixParameters()
                # format the values for the zabbix server.
                all_values = converter.format(parameters)
                # generate string for of all the values in a zabbix_sender format.
                zabbix_formatted = converter.parameters(all_values)
                sender = ZabbixSender(cmds.get('zabbix_binaries'))
                # send data to the zabbix server.
                result = sender.send(cmds.get('zabbix_server'),
                                     converter.get_client(all_values),
                                     zabbix_formatted,
                                     cmds.get('debug_send'))
                if result is True:
                    self._logger.info(
                        "Data successfully sent to the zabbix server.")
                else:
                    self._logger.warning(
                        "Data was not successfully sent to the zabbix server.")
            except IOError as ioe:
                self._logger.exception("IOException")
            except Exception as e:
                self._logger.exception("Exception")
        except Exception as e_e:
            print("Something went wrong setting up the logger.")
            print(str(e_e))


if __name__ == "__main__":
    main = Main()
    main.main()
