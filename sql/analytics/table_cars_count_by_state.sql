SELECT count(*) as "Quantidade", te.uf as "UF"
FROM tb_carro tca
INNER JOIN tb_cidade tc on tc.id = tca.cidade_id
INNER JOIN tb_estado te on te.id = tc.estado_id
GROUP BY te.uf
ORDER BY "Quantidade" DESC
