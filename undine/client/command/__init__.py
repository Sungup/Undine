from undine.client.command.dashboard import Dashboard
from undine.client.command.mission import Mission
from undine.client.command.task import Task, Config, Input, Worker


class CommandFactory:
    _COMMANDS = {
        'dashboard': Dashboard,
        'mission': Mission,
        'task': Task,
        'config': Config,
        'input': Input,
        'worker': Worker
    }

    @staticmethod
    def add_subparsers(parser, dest, description):
        subparsers = parser.add_subparsers(help = description, dest = dest)

        for name, Class in CommandFactory._COMMANDS.items():
            Class.add_arguments(
                subparsers.add_parser(name, help = Class.help()))

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
