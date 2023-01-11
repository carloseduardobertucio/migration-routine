CREATE TABLE IF NOT EXISTS usuario (
	id_usuario serial PRIMARY KEY NOT NULL,
	nome varchar(100) NOT NULL,
	email varchar(200) UNIQUE NOT NULL,
	estado varchar(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS produto (
	id_produto serial PRIMARY KEY NOT NULL,
	nome varchar(100) UNIQUE NOT NULL,
	valor numeric(5,2) NOT NULL,
	categoria varchar(100) NOT NULL
);

CREATE TYPE status_venda AS ENUM ('Faturado', 'Cancelado');

CREATE TABLE IF NOT EXISTS venda (
	id_venda serial PRIMARY KEY NOT NULL,
	data_venda date NOT NULL,
	status status_venda NOT NULL,
	id_usuario integer NOT NULL,
	id_produto integer NOT NULL,
	CONSTRAINT venda_usuario FOREIGN KEY (id_usuario) REFERENCES usuario(id_usuario),
	CONSTRAINT venda_produto FOREIGN KEY (id_produto) REFERENCES produto(id_produto)
);