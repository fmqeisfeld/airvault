from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

class Link_Loader(BaseOperator):  
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 link:str,                           
                 *args,
                 **kwargs):
                
        super().__init__(*args, **kwargs)
                             
        schema_edwh = Variable.get('SCHEMA_EDWH')
        schema_stage = Variable.get('SCHEMA_STAGE')

        self.sql= open('/opt/airflow/dags/sql/link_loader.sql','r').read()
        appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface        
        sql_concat=''

        # collect bks -> hks mappping from all sources
        bk_list=list() # total
        hk_list=list() # total
        bk_hk_obj={} # per source

        for cur_src, cur_conf in conf['links'][link]['src'].items():
            
            #link_bks_dict={**link_bks_dict,**cur_conf['bks']} # merge dicts
            #print(link_bks_dict)
            bk_list +=[x for x in cur_conf['bks'].keys()]
            hk_list +=[x for x in cur_conf['bks'].values()]
            bk_hk_obj[cur_src]=cur_conf['bks']


        # das brauche ich
        # ich möchte, dass der user in conf später irgendwann auch die reihenfolge seiner quellen und die bks pro quelle ändern darf.
        # deswegen brauche ich diese sortierten dicts
        bk_hk_dict = dict(zip(bk_list,hk_list))
        bk_hk_dict_sorted=dict(sorted(bk_hk_dict.items()))
        hk_bk_dict_sorted=dict(zip([x for x in bk_hk_dict_sorted.values()],[x for x in bk_hk_dict_sorted.keys()]))

        hk_unique = [ x for x in hk_bk_dict_sorted.keys() if x != None]
        hk_list_keys_str = ','.join(hk_unique)

        bk_list_keys = list(hk_bk_dict_sorted.values())
        bk_list_keys_str = ','.join(bk_list_keys)


        for cur_src,cur_conf in conf['links'][link]['src'].items():
            
            tenant= cur_conf['tenant'] if cur_conf['tenant'] else "default"
            bkeycode = cur_conf['bkeycode'] if cur_conf['bkeycode'] else "default"

            cur_obj=bk_hk_obj[cur_src]
            cur_obj_inv=dict(zip(cur_obj.values(),cur_obj.keys()))

            # hk_list für INSERT
            hk_list_vals = list(map(lambda x: 
                    f"md5(CONCAT_WS('|',\'{tenant}\',\'{bkeycode}\', trim({x}::varchar(256))))",
                    [cur_obj_inv[x] for x in hk_unique]))

            hk_list_vals_str=',\n'.join(hk_list_vals)

            # bk_list für INSERT
            bk_list_vals = [cur_obj_inv[y] if y in cur_obj_inv.keys() else "\'-1\'" for y in hk_bk_dict_sorted.keys()]
            bk_list_vals_str="::varchar(256),\n".join(bk_list_vals) + "::varchar(256)"

            concat_list = [cur_obj_inv[y] for y in hk_bk_dict_sorted.keys() if y in cur_obj_inv.keys() and y]
            concat_list_str =f"concat_ws('|',\'{tenant}\',\'{bkeycode}\'," +','.join(list(map(lambda x:f'{x}::varchar(256)',concat_list)))+')'

            
            params={'tenant': tenant,
                    'bkeycode':bkeycode,
                    'taskid': self.task_id,
                    'appts':'appts::timestamp' if appts else 'current_timestamp',
                    'rec_src':cur_src,
                    #            
                    'schema_src':schema_stage,
                    'table_src':cur_src,
                    'schema_tgt':schema_edwh,
                    'table_tgt':link,
                    #
                    'hk_list_keys':hk_list_keys_str,
                    'bk_list_keys':bk_list_keys_str,
                    'hk_list_vals':hk_list_vals_str,
                    'bk_list_vals':bk_list_vals_str, 
                    'concat_str':concat_list_str,
                    'hk_tgt':conf['links'][link]['hk']
            }

            #print(sql.format(**params))
            sql_concat += '\n'+ self.sql.format(**params)


        self.sql = sql_concat
        self.doc=self.sql


    def execute(self, context: Context):        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(self.sql)

        
            
        