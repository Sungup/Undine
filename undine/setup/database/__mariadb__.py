from undine.setup.database.__core__ import DBInitializer, LoadItems
from undine.database.mariadb import MariaDbConnector
from undine.database.rabbitmq import RabbitMQConnector
from undine.utils.exception import UndineException


_MARIADB_CREATE = {
    'table': {
        'state_type': '''
            create table state_type
            (
              state varchar(1)  not null primary key,
              name  varchar(16) not null
            );
        ''',
        'mission': '''
            create table mission
            (
              mid         char(32)     not null primary key,
              name        varchar(255) not null,
              email       varchar(254) null,
              description longtext     not null,
              issued      datetime(6)  not null
            );
        ''',
        'config': '''
            create table config
            (
              cid    char(32)     not null primary key,
              name   varchar(255) not null,
              config longtext     not null,
              issued datetime(6)  not null
            );
        ''',
        'input': '''
            create table input
            (
              iid    char(32)     not null primary key,
              name   varchar(255) not null,
              items  longtext     not null,
              issued datetime(6)  not null
            );
        ''',
        'worker': '''
            create table worker
            (
              wid        char(32)     not null primary key,
              name       varchar(255) not null,
              command    varchar(255) not null,
              arguments  varchar(255) not null,
              worker_dir varchar(255) not null,
              file_input tinyint(1)   not null,
              issued     datetime(6)  not null
            );
        ''',
        'task': '''
            create table task
            (
              tid        char(32)     not null primary key,
              mid        char(32)     not null,
              cid        char(32)     not null,
              iid        char(32)     not null,
              wid        char(32)     not null,
              name       varchar(255) not null,
              reportable tinyint(1)   not null,
              state      varchar(1)   not null,
              issued     datetime(6)  not null,
              updated    datetime(6)  null,
              host       varchar(255) not null,
              ip         char(39)     not null,
              constraint task_cid_fk_config_cid
                  foreign key (cid) references config (cid),
              constraint task_iid_fk_input_iid
                  foreign key (iid) references input (iid),
              constraint task_mid_fk_mission_mid
                  foreign key (mid) references mission (mid),
              constraint task_state_fk_state_type_state
                  foreign key (state) references state_type (state),
              constraint task_wid_fk_worker_wid
                  foreign key (wid) references worker (wid)
            );
        ''',
        'error': '''
            create table error
            (
              tid      char(32)    not null primary key,
              message  longtext    not null,
              informed datetime(6) not null,
              constraint error_tid_fk_task_tid
                  foreign key (tid) references task (tid)
            );
        ''',
        'result': '''
            create table result
            (
              tid      char(32)    not null primary key,
              content  longtext    not null,
              reported datetime(6) not null,
              constraint result_tid_fk_task_tid
                  foreign key (tid) references task (tid)
            );
        ''',
        'trash': '''
            create table trash
            (
              id        int auto_increment primary key,
              tid       char(32)    not null,
              category  varchar(16) not null,
              content   longtext    not null,
              generated datetime(6) not null,
              trashed   datetime(6) not null,
              constraint trash_tid_fk_task_tid
                  foreign key (tid) references task (tid)
            );
        ''',
        'host': '''
            create table host
            (
              ip         char(39)     not null primary key,
              name       varchar(255) not null,
              registered datetime(6)  not null,
              logged_in  datetime(6)  null,
              logged_out datetime(6)  null
            );
        '''
    },
    'index': {
        'task_ip_state_for_host_list_view': '''
            create index task_ip_state_for_host_list_view on task (ip, state);
        ''',
        'task_mid_state_for_task_list_view': '''
            create index task_mid_state_for_task_list_view on task (mid, state);
        '''
    },
    'view': {
        'mission_dashboard': '''
            create view mission_dashboard as
              select m.mid                                       AS mid,
                     m.name                                      AS name,
                     m.email                                     AS email,
                     m.description                               AS description,
                     count(case when t.state = 'R' then 1 end)   AS ready,
                     count(case when t.state = 'I' then 1 end)   AS issued,
                     count(case when t.state = 'D' then 1 end)   AS done,
                     count(case when t.state = 'C' then 1 end)   AS canceled,
                     count(case when t.state = 'F' then 1 end)   AS failed,
                     !count(case when t.state <> 'D' then 1 end) AS complete,
                     m.issued                                    AS issued_at
              from mission m join task t
              where m.mid = t.mid
              group by m.mid
              order by m.issued desc;
        ''',
        'host_list': '''
            create view host_list as
              select h.name                                        AS name,
                     h.ip                                          AS ip,
                     count(case when t.state = 'I' then 1 end)     AS issued,
                     count(case when t.state = 'C' then 1 end)     AS canceled,
                     count(case when t.state = 'F' then 1 end)     AS failed,
                     h.registered                                  AS registered,
                     h.logged_in                                   AS logged_in,
                     h.logged_out                                  AS logged_out,
                     if(h.logged_in < h.logged_out, 'Offline', '') AS state
              from (host h left join task t on (h.ip = t.ip))
              group by h.name, h.ip, h.registered, h.logged_in, h.logged_out
              order by h.ip;
        ''',
        'task_list': '''
            create view task_list as
              select t.mid        AS mid,
                     t.tid        AS tid,
                     t.name       AS name,
                     t.host       AS host,
                     t.ip         AS ip,
                     t.issued     AS issued,
                     t.updated    AS updated,
                     s.name       AS state,
                     t.reportable AS reportable,
                     t.cid        AS cid,
                     t.iid        AS iid,
                     t.wid        AS wid
              from task t join state_type s
              where t.state = s.state
              order by t.issued;
        '''
    }
}

_MARIADB_DROP = {
    'view': ['mission_dashboard', 'host_list', 'task_list'],
    'table': ['host', 'trash', 'result', 'error',
              'task', 'worker', 'input', 'config', 'mission', 'state_type'],
}

_MARIADB_LOAD = {
    'state_type': LoadItems(
        'INSERT INTO state_type VALUES (%s, %s)',
        [
            ('R', 'ready'),
            ('I', 'issued'),
            ('D', 'done'),
            ('C', 'canceled'),
            ('F', 'failed')
        ]
    )
}


class MariaDBInitializer(DBInitializer):
    def __init__(self, configs):
        super(self.__class__, self).__init__(configs)

        # Check configuration info
        if 'task_queue' not in self.config:
            raise UndineException("Task queue information doesn't exist")

        # Make clean configuration info and check it's the mariadb config.
        db_config = 'driver' if 'driver' in self.config else 'database'
        db_config = {k: v
                     for k, v in self.config[db_config].items()
                     if k in ['host', 'database', 'user', 'password']}

        # Connect to database
        self.__db = MariaDbConnector(db_config)
        self.__mq_config = self.config['task_queue']

    def build_table(self):
        # 1. First of all drop all table, index, and view
        self.__db.execute_multiple_dml(
            [self.__db.sql('DROP {} IF EXISTS {}'.format(obj.upper(), name))
             for obj, tables in _MARIADB_DROP.items()
             for name in tables]
        )

        # 2. Create tables, indexes, and views
        self.__db.execute_multiple_dml(
            [self.__db.sql(query)
             for obj, queries in _MARIADB_CREATE.items()
             for _, query in queries.items()]
        )

    def load_initial_data(self):
        # 1. Load data
        self.__db.execute_multiple_dml(
            [self.__db.sql(items.query, *value)
             for table, items in _MARIADB_LOAD.items()
             for value in items.values]
        )

    def post_initialization(self):
        _ = RabbitMQConnector(self.__mq_config, consumer=False, rebuild=True)
