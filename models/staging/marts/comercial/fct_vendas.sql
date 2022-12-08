with
    clientes as (
        select
        from {{ ref('dim_clientes')}}
    )

    , funcionarios as (
        select
        from {{ ref('dim_funcionarios')}}
    )

    , produtos as (
        select
        from {{ ref('dim_produtos')}}
    )

    , transportadoras as (
        select
        from {{ ref('dim_transportadoras')}}
    )

    , 