# 010-rede2018-candidaturas_2018


```python
ano_eleicao = '2018'
dbschema = f'rede{ano_eleicao}'
table_candidaturas =  f"{dbschema}.candidaturas_{ano_eleicao}"
table_consulta_cand = f"tse{ano_eleicao}.consulta_cand_{ano_eleicao}"
table_despesas_candidatos = f"tse{ano_eleicao}.despesas_contratadas_candidatos_{ano_eleicao}"
table_receitas_candidatos = f"tse{ano_eleicao}.receitas_candidatos_{ano_eleicao}"
table_votacao_candidato_munzona  = f"tse{ano_eleicao}.votacao_candidato_munzona_{ano_eleicao}"
```


```python
import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]
```


```python
print(mtse.pandas_query(f'select count(*) as "total de registos consulta_cand" from {table_consulta_cand}').to_string(index=False))
```


```python
local_dir = f'{home}/temp'
```

## TABELA CANDIDATURAS


```python
query_create_table_candidaturas = F"""
drop table if exists {table_candidaturas} cascade;

-- Atributos obtidos da tabela do TSE consulta_cand
create table {table_candidaturas}
(
    dt_geracao                    varchar,
    hh_geracao                    varchar,
    ano_eleicao                   varchar,
    cd_tipo_eleicao               varchar,
    nm_tipo_eleicao               varchar,
    nr_turno                      varchar,
    cd_eleicao                    varchar,
    ds_eleicao                    varchar,
    dt_eleicao                    varchar,
    tp_abrangencia                varchar,
    sg_uf                         varchar,
    sg_ue                         varchar,
    nm_ue                         varchar,
    cd_cargo                      varchar,
    ds_cargo                      varchar,
    sq_candidato                  varchar,
    nr_candidato                  varchar,
    nm_candidato                  varchar,
    nm_urna_candidato             varchar,
    nm_social_candidato           varchar,
    nr_cpf_candidato              varchar,
    nm_email                      varchar,
    cd_situacao_candidatura       varchar,
    ds_situacao_candidatura       varchar,
    cd_detalhe_situacao_cand      varchar,
    ds_detalhe_situacao_cand      varchar,
    tp_agremiacao                 varchar,
    nr_partido                    varchar,
    sg_partido                    varchar,
    nm_partido                    varchar,
    sq_coligacao                  varchar,
    nm_coligacao                  varchar,
    ds_composicao_coligacao       varchar,
    cd_nacionalidade              varchar,
    ds_nacionalidade              varchar,
    sg_uf_nascimento              varchar,
    cd_municipio_nascimento       varchar,
    nm_municipio_nascimento       varchar,
    dt_nascimento                 varchar,
    nr_idade_data_posse           varchar,
    nr_titulo_eleitoral_candidato varchar,
    cd_genero                     varchar,
    ds_genero                     varchar,
    cd_grau_instrucao             varchar,
    ds_grau_instrucao             varchar,
    cd_estado_civil               varchar,
    ds_estado_civil               varchar,
    cd_cor_raca                   varchar,
    ds_cor_raca                   varchar,
    cd_ocupacao                   varchar,
    ds_ocupacao                   varchar,
    nr_despesa_max_campanha       numeric(18,2),
    cd_sit_tot_turno              varchar,
    ds_sit_tot_turno              varchar,
    st_reeleicao                  varchar,
    st_declarar_bens              varchar,
    nr_protocolo_candidatura      varchar,
    nr_processo                   varchar,
   --------------------------------------------- 
    candidato_id                  varchar,
    candidato_label               varchar,
    nr_cnpj_prestador_conta       varchar,
    apto                          varchar,
    total_votos_turno_1           numeric,
    total_votos_turno_2           numeric,
    declarou_receita              varchar,
    receita_total                 numeric(18,2),
    declarou_despesa              varchar,
    despesa_total                 numeric(18,2),
    tse_id                        varchar
 );

CREATE INDEX ON {table_candidaturas} (candidato_id);

CREATE INDEX ON {table_candidaturas} (nm_candidato);

CREATE INDEX ON {table_candidaturas} (nm_urna_candidato);

CREATE INDEX  IF NOT EXISTS sq_candidato_idx ON {table_candidaturas} ( sq_candidato );
"""
```

## Insere os dados de consulta_cand 


```python
query_insert_candidaturas = f"""
INSERT INTO {table_candidaturas} 
(SELECT
    dt_geracao                    ,
    hh_geracao                    ,
    ano_eleicao                   ,
    cd_tipo_eleicao               ,
    nm_tipo_eleicao               ,
    nr_turno                      ,
    cd_eleicao                    ,
    ds_eleicao                    ,
    dt_eleicao                    ,
    tp_abrangencia                ,
    sg_uf                         ,
    sg_ue                         ,
    nm_ue                         ,
    cd_cargo                      ,
    ds_cargo                      ,
    sq_candidato                  ,
    nr_candidato                  ,
    nm_candidato                  ,
    nm_urna_candidato             ,
    nm_social_candidato           ,
    nr_cpf_candidato              ,
    nm_email                      ,
    cd_situacao_candidatura       ,
    ds_situacao_candidatura       ,
    cd_detalhe_situacao_cand      ,
    ds_detalhe_situacao_cand      ,
    tp_agremiacao                 ,
    nr_partido                    ,
    sg_partido                    ,
    nm_partido                    ,
    sq_coligacao                  ,
    nm_coligacao                  ,
    ds_composicao_coligacao       ,
    cd_nacionalidade              ,
    ds_nacionalidade              ,
    sg_uf_nascimento              ,
    cd_municipio_nascimento       ,
    nm_municipio_nascimento       ,
    dt_nascimento                 ,
    nr_idade_data_posse           ,
    nr_titulo_eleitoral_candidato ,
    cd_genero                     ,
    ds_genero                     ,
    cd_grau_instrucao             ,
    ds_grau_instrucao             ,
    cd_estado_civil               ,
    ds_estado_civil               ,
    cd_cor_raca                   ,
    ds_cor_raca                   ,
    cd_ocupacao                   ,
    ds_ocupacao                   ,
    nr_despesa_max_campanha::numeric       ,
    cd_sit_tot_turno              ,
    ds_sit_tot_turno              ,
    st_reeleicao                  ,
    st_declarar_bens              ,
    nr_protocolo_candidatura      ,
    nr_processo_,  

    get_candidato_id(c.sg_uf,c.sq_candidato) AS candidato_id,
    get_candidato_label(c.nm_candidato,c.ds_cargo,c.sg_uf,c.sg_partido) as candidato_label,
    '' as nr_cnpj_prestador_conta,
    eh_candidato_apto(c.cd_situacao_candidatura) as apto,
    0 as     total_votos_turno_1,
    0 as     total_votos_turno_2,
    'N' as declarou_receita,
    0 as receita_total,
    'N' as declarou_despesa,
    0 as despesa_total,
    get_tse_id(sg_uf,sq_candidato) as tse_id
from
   {table_consulta_cand} as c
where
    c.nr_turno = '1' 
  and 
    c.ds_situacao_candidatura='APTO'
  -- and
  -- eh_candidato_titular(c.cd_cargo) = 'S'
)
;
"""

mtse.execute_query(query_create_table_candidaturas)
mtse.execute_query(query_insert_candidaturas)

```

###  GERA CNPJ A PARTIR DA DECLARAÇÃO DE RECEITAS


```python
query_update_cnpj_a_partir_receitas = f"""
    with receitas_candidatos as
    (
    SELECT 
      sq_candidato, 
      nr_cnpj_prestador_conta,
      round(sum(vr_receita),2) as receita_total, 
      get_tse_id(sg_uf,sq_candidato) as tse_id 
    FROM 
      {table_receitas_candidatos}
    group by
      sq_candidato,
      nr_cnpj_prestador_conta,
      tse_id 
    )
    update {table_candidaturas} as c
      set nr_cnpj_prestador_conta = r.nr_cnpj_prestador_conta,
          declarou_receita = 'S',
          receita_total = r.receita_total
    from 
      receitas_candidatos as r
    where
      c.tse_id = r.tse_id 
    ;  
"""

mtse.execute_query(query_update_cnpj_a_partir_receitas)
```

###  GERA CNPJ (faltantes) A PARTIR DA DECLARAÇÃO DE RECEITAS


```python
query_update_cnpj_a_partir_despesas = f"""
    with despesas_candidatos as
    (
    SELECT 
      sq_candidato, 
      nr_cnpj_prestador_conta,
      round(sum(vr_despesa_contratada),2) as despesa_total,
      get_tse_id(sg_uf,sq_candidato) as tse_id  
    FROM 
      {table_despesas_candidatos}
    group by
      sq_candidato, 
      nr_cnpj_prestador_conta,
      tse_id
    )
    update {table_candidaturas} as c
      set nr_cnpj_prestador_conta = d.nr_cnpj_prestador_conta,
          declarou_despesa = 'S',
          despesa_total = d.despesa_total
    from 
      despesas_candidatos as d
    where
      c.tse_id = d.tse_id
    ;
"""

mtse.execute_query(query_update_cnpj_a_partir_despesas)
```

### Gra total votos turno 1


```python
query_atualiza_total_votos_turno_1 = f"""
    with votos_turno_1 as 
    (
      select
       get_tse_id(sg_uf,sq_candidato) as tse_id,
       sum(qt_votos_nominais::numeric) as total_votos
      from 
        {table_votacao_candidato_munzona} as v1
      where 
        nr_turno = '1'
      group by 
        tse_id
    )
    update {table_candidaturas} as c
    set 
       total_votos_turno_1 = v1.total_votos
    from 
       votos_turno_1 as v1
    where 
       c.tse_id = v1.tse_id
    ;
    """

mtse.execute_query(query_atualiza_total_votos_turno_1)
print(f'Incluido total de votos do turno 1')
```

### Gera total votos turno 2


```python
query_atualiza_total_votos_turno_2 = f"""
    with votos_turno_2 as 
    (
      select
        get_tse_id(sg_uf,sq_candidato) as tse_id,
        sum(qt_votos_nominais::numeric) as total_votos
      from 
        {table_votacao_candidato_munzona} as v2
      where 
        nr_turno = '2'
      group by 
        tse_id
    )
    update {table_candidaturas} as c
    set 
       total_votos_turno_2 = v2.total_votos
    from 
       votos_turno_2 as v2
    where 
       c.tse_id = v2.tse_id
    ;
"""
    
mtse.execute_query(query_atualiza_total_votos_turno_2)
print(f'Incluido total de votos do turno 2')
```
