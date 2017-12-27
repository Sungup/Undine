import platform
import multiprocessing
import subprocess


class System:
    __window_gw_query__ = '@for /f "token=3" %j in ' \
                            '(\'route print ^|findstr "\\<0.0.0.0\\>"\') ' \
                            'do @echo %j'

    __mac_gw_query__ = (('route', '-n', 'get', 'default'),
                        ('awk', '/gateway/ { print $2 }'))

    __linux_gw_query__ = (('ip', 'route', 'show', 'default'),
                          ('awk', '/default/ { print $3 }'))

    @staticmethod
    def is_window():
        return platform.system() == 'Windows'

    @staticmethod
    def is_linux():
        return platform.system() == 'Linux'

    @staticmethod
    def is_mac():
        return platform.system() == 'Darwin'

    @staticmethod
    def cpu_cores():
        return multiprocessing.cpu_count()

    @staticmethod
    def default_gateway():
        # Select system dependent gw query string
        if System.is_window():
            raise Exception('Currently windows platform not in support')
        elif System.is_mac():
            query = System.__mac_gw_query__
        else:
            query = System.__linux_gw_query__

        # Query the network device
        net_query = subprocess.Popen(query[0], stdout=subprocess.PIPE)

        # After processing to grepping the default gw address
        output = subprocess.check_output(query[1], stdin=net_query.stdout)

        net_query.wait()

        return output.strip()
