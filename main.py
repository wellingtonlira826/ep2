import scrapy
import os
from neo4j import GraphDatabase
from scrapy.exceptions import CloseSpider
import time

'''
Grupo : Christian Dambock Gomes , Guilherme Lozano Borges, Wellington Lira
'''
# Classe de Conexão com o Neo4j
class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Falha ao criar o driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver não inicializado!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Consulta falhou:", e)
        finally: 
            if session is not None:
                session.close()
        return response
    

# Inicializa a conexão com o Neo4j
conn = Neo4jConnection(uri=os.environ['NEO4J_URI'], 
                       user=os.environ['NEO4J_USER'], 
                       pwd=os.environ['NEO4J_PWD'])


# Spider do Scrapy
class MovieSpider(scrapy.Spider):

  name = 'moviespider'
  start_urls = [
    'https://editorial.rottentomatoes.com/guide/best-horror-movies-of-all-time/'
  ]

  def parse(self, response):
    
    # Sistemas de Recomendação de Filmes
    movie_name_to_search = input("Escolha um filme, e o sistema irá recomendar os 5   top filmes parecidos: ")
    find_similar_movies(movie_name_to_search)
    time.sleep(10)  # Delay de 10 segundos para atualizar o Neo4j 
    
    # Inicio do scraping
    linhas = response.css('div.article_movie_title')
    for linha in linhas:
      link = linha.css("div > h2 > a::attr(href)")
      porcentagem = linha.css('div > h2 > span.tMeterScore::text').get()
      ano = linha.css('div > h2 > span.subtle.start-year::text').get()
      yield response.follow(link.get(),
                            self.parser_movie,
                            meta={
                              "porcentagem": porcentagem,
                              "ano": ano
                            })

  def parser_movie(self, response):
    nome = response.css(
      'div.thumbnail-scoreboard-wrap > score-board-deprecated > h1::text').get(
      )

    genero = response.css('ul#info > li:nth-child(2) > p > span::text').get()

    diretor = response.css(
      'ul#info > li:nth-child(4) > p > span > a::text').get()
    lacamento = response.css(
      'ul#info > li:nth-child(7) > p > span > time::text').get()

    bilheteria = response.css(
      'ul#info > li:nth-child(10) > p > span::text').get()
    duracao = response.css(
      'ul#info > li:nth-child(11) > p > span > time::text').get()
    watch = response.css(
      'section.where-to-watch > bubbles-overflow-container > where-to-watch-meta >where-to-watch-bubble::attr(image) '
    ).getall()

    porcentagem = response.meta['porcentagem']
    ano = response.meta['ano']
    
    yield {
      "Nome":
      nome,
      "Porcentagem":
      porcentagem,
      "Ano que Saiu":
      ano.replace("(", "").replace(")", ""),
      "Diretor":
      diretor,
      "Data que Saiu":
      lacamento,
      "Bilheteria":
      bilheteria.replace("\n", "").replace(" ", ""),
      "Tamanho do Filme":
      duracao if duracao is None else duracao.replace("\n", "").replace(
        " ", ""),
      "Genero":
      genero.replace("\n", "").replace(" ", ""),
      "Aonde Assistir":
      watch,
    }

    # Consulta para excluir nós e relacionamentos existentes com o mesmo nome e ano
    delete_movie_query = '''
    MATCH (movie:Movie {name: $nome, year: $ano})
    DETACH DELETE movie
    '''

    # Executa a exclusão antes de criar os novos nós e relacionamentos
    conn.query(delete_movie_query, parameters={
        'nome': nome,
        'ano': ano.replace("(", "").replace(")", "")
    })

    
    # Criação do nó do filme e dos relacionamentos no Neo4j
    create_movie_query = '''
    MERGE (movie:Movie {name: $nome, year: $ano})
    ON CREATE SET movie.bilheteria = $bilheteria, movie.duracao = $duracao,     movie.porcentagem = $porcentagem
  
    WITH movie
    MERGE (genre:Genre {name: $genero})
    MERGE (movie)-[:IS_OF_GENRE]->(genre)
  
    WITH movie
    MERGE (director:Director {name: $diretor})
    MERGE (movie)-[:DIRECTED_BY]->(director)
    '''
  
    # Execução da query
    conn.query(create_movie_query, parameters={
        'nome': nome,
        'ano': ano.replace("(", "").replace(")", ""),
        'bilheteria': bilheteria.replace("\n", "").replace(" ", ""),
        'duracao': duracao if duracao is None else duracao.replace("\n", "").replace(" ", ""),
        'genero': genero.replace("\n", "").replace(" ", ""),
        'diretor': diretor,
        'porcentagem': float(porcentagem.replace("%", "").strip()),
    })

    
    # Encerramento da conexão
    conn.close()

def find_similar_movies(movie_name):
  
  # Consulta dos 5 filmes de acordo com o genero
  similar_movies_query = '''
  MATCH (movie:Movie)-[:IS_OF_GENRE]->(genre)<-[:IS_OF_GENRE]-(similar:Movie)
  WHERE movie.name = $movie_name
  AND toFloat(movie.porcentagem) <= toFloat(similar.porcentagem) + 10
  AND toFloat(movie.porcentagem) >= toFloat(similar.porcentagem) - 10
  RETURN similar.name AS name, similar.porcentagem AS percentage
  ORDER BY toFloat(similar.porcentagem) DESC
  LIMIT 5
  '''

  results = conn.query(similar_movies_query, parameters={'movie_name': movie_name})

  # Utilize o Sheel para visualizar o Sistemas de Recomendação de Filmes (comando python main.py)
  print(f"Filmes similares a {movie_name}: \n")
  for record in results:
      print(f"{record['name']} - {record['percentage']}%")
      print("\n")