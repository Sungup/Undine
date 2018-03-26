from undine.client.command.dashboard import Dashboard
from undine.client.command.host import Host
from undine.client.command.mission import Mission
from undine.client.command.task import Task, Config, Input, Worker


class CommandFactory:
    _COMMANDS = {
        'dashboard': Dashboard,
        'mission': Mission,
        'host': Host,
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

    @staticmethod
    def get_command(command, config, connector):
        return CommandFactory._COMMANDS[command](config, connector)
