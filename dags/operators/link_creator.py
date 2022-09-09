from airflow.models import BaseOperator
from airflow.models.taskinstance import Context

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models import Variable

class Link_Creator(BaseOperator):  
    #@apply_defaults
    def __init__(self,
                 conf:dict,
                 link:str,                                   
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.link=link
        self.sql= open('/opt/airflow/dags/sql/link_creator.sql','r').read()

        schema_edwh = Variable.get('SCHEMA_EDWH')
        
        # collect bks -> hks mappping from all sources
        bk_list=list()
        hk_list=list()

        for cur_src, cur_conf in conf['links'][link]['src'].items():            
            bk_list +=[x for x in cur_conf['bks'].keys()]
            hk_list +=[x for x in cur_conf['bks'].values()]

        # das brauche ich
        # ich möchte, dass der user in conf später irgendwann auch die reihenfolge seiner quellen und die bks pro quelle ändern darf.
        # deswegen brauche ich diese sortierten dicts

        bk_hk_dict = dict(zip(bk_list,hk_list))
        bk_hk_dict_sorted=dict(sorted(bk_hk_dict.items()))
        hk_bk_dict_sorted=dict(zip([x for x in bk_hk_dict_sorted.values()],[x for x in bk_hk_dict_sorted.keys()]))

        hk_unique = [ f'{x} varchar(256) NOT NULL' for x in hk_bk_dict_sorted.keys() if x != None]
        hk_list_create_str = ','.join(hk_unique)

        bk_list_create = [ f'{x} varchar(256) NOT NULL' for x in hk_bk_dict_sorted.values()]
        bk_list_create_str = ','.join(bk_list_create)


        params = {
            'schema':schema_edwh,
            'link':link,
            'hk':conf['links'][link]['hk'],
            'bk_list_create':bk_list_create_str,
            'hk_list_create':hk_list_create_str
        }     

        self.sql = self.sql.format(**params)
        self.doc = self.sql

    def execute(self, context: Context): 
        vault_tables = Variable.get('vault_tables')

        if not self.link in vault_tables:
            self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
            self.hook.run(self.sql)            