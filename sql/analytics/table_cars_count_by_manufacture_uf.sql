select tm.marca, te.uf, count(tm.marca) as "Quantidade" from tb_carro tca
inner join tb_cidade tc on tca.cidade_id = tc.id
inner join tb_estado te on tc.estado_id = te.id 
inner join tb_montadora tm on tca.montadora_id = tm.id
GROUP BY tm.marca, te.uf
ORDER BY tm.marca