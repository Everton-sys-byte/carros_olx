select tm.marca, count(*) as "Quantidade" from tb_carro tca
inner join tb_montadora tm on tca.montadora_id = tm.id
GROUP BY tm.marca


