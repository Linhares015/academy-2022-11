with
    pedidos as (
        select 
            id_pedido			
            , id_cliente			
            , id_funcionario			
            , data_do_pedido			
            , data_requerida			
            , data_do_envio			
            , id_transportadora			
            , frete			
            , destinatario			
            , endereco_destinatario			
            , cidade_destinatario			
            , regiao_destinatario			
            , cep_destinatario			
            , pais_destinatario	
        from {{ ref('stg_erp__ordens')}}
    )

    , pedido_item as (
        select 
            id_pedido				
            , id_produto				
            , desconto				
            , preco_da_unidade				
            , quantidade		
        from {{ ref('stg_erp__ordens_detalhes')}}
    )

    , joined as (
        select 
            pedidos.id_pedido			
            , pedidos.id_cliente			
            , pedidos.id_funcionario			
            , pedidos.data_do_pedido			
            , pedidos.data_requerida			
            , pedidos.data_do_envio			
            , pedidos.id_transportadora			
            , pedidos.frete			
            , pedidos.destinatario			
            , pedidos.endereco_destinatario			
            , pedidos.cidade_destinatario			
            , pedidos.regiao_destinatario			
            , pedidos.cep_destinatario			
            , pedidos.pais_destinatario	
            , pedido_item.preco_da_unidade				
            , pedido_item.desconto				
            , pedido_item.quantidade
            , pedido_item.id_produto	
        from pedidos
        left join pedido_item on pedidos.id_pedido = pedido_item.id_pedido 
    )


select *
from joined