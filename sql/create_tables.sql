CREATE TABLE IF NOT EXISTS tb_estado (
    id serial primary key,
    uf char(2) unique not null 
);

CREATE TABLE IF NOT EXISTS tb_cidade (
    id serial primary key,
    cidade varchar(55) unique not null,
    estado_id int,
    CONSTRAINT fk_state FOREIGN KEY (estado_id) REFERENCES tb_estado(id)
);

CREATE TABLE IF NOT EXISTS tb_montadora (
    id serial primary key,
    marca varchar(55) unique not null
);

CREATE TABLE IF NOT EXISTS tb_carro (
    id serial primary key,
    titulo_anuncio text unique,
    categoria text,
    modelo text,
    tipo_de_veiculo varchar(55),
    ano varchar(20),
    quilometragem varchar(55),
    potencia_do_motor varchar(30),
    combustivel varchar(55),
    cambio varchar(55),
    direcao varchar(55),
    cor varchar(55),
    preco decimal(10,2),
    cidade_id int,
    montadora_id int,
    CONSTRAINT fk_city FOREIGN KEY (cidade_id) REFERENCES tb_cidade(id),
    CONSTRAINT fk_manufatcure FOREIGN KEY (montadora_id) REFERENCES tb_montadora(id)
);