from undine.client.command.dashboard import Dashboard
from undine.client.command.missions import Missions
from undine.client.command.tasks import Tasks


class CommandFactory:
    _COMMANDS = {
        'dashboard': Dashboard,
        'missions': Missions,
        'tasks': Tasks
    }

    @staticmethod
    def add_subparsers(parser, dest, description):
        subparsers = parser.add_subparsers(help=description, dest=dest)

        for name, Class in CommandFactory._COMMANDS.items():
            Class.add_arguments(subparsers.add_parser(name, help=Class.help()))

        # TODO Add additional parser
        """
        Following commands will added near future
        
        # Nodes command
        nodes = subparsers.add_parser('nodes', help='List-up service nodes')

        # Stats command
        stats = subparsers.add_parser('stats', help='Service node stats')
        """

    @staticmethod
    def get_command(command, config, connector):
        return CommandFactory._COMMANDS[command](config, connector)
