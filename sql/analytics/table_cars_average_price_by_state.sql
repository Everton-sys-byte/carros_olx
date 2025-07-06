select round(avg(preco)) as "Preço Médio", te.uf as "UF" from tb_carro as tc
INNER JOIN tb_cidade tci on tc.cidade_id = tci.id
INNER join tb_estado te on te.id = tci.estado_id
GROUP BY te.uf
ORDER BY "Preço Médio" desc

