package pvehiculos2;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import db.OracleDB;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.bson.Document;
import pvehiculos.Clientes;
import pvehiculos.Vehiculos;

public class Main {

    public static void main(String[] args) {
        OracleDB.getSession().setAutoclose(false);

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("src/main/resources/pvehiculos/vehicli.odb");
        EntityManager em = emf.createEntityManager();

        MongoClient mongoClient = new MongoClient(OracleDB.SERVER_HOSTNAME, 27017);
        MongoDatabase mongoDB = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = mongoDB.getCollection("vendas");

        try (Statement statement = OracleDB.getSession().getConn().createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM FINALVEH");
            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String dni = resultSet.getString(2);

                //objeto>
                String codveh = resultSet.getString(3);
                int tasa = resultSet.getInt(4);


            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        FindIterable<Document> docs = collection.find();
        for (Document document : docs) {
            System.out.println(document);
            int id = document.getDouble("_id").intValue();
            String dni = document.getString("dni");
            String codveh = document.getString("codveh");

            TypedQuery<Clientes> queryClientes = em.createQuery("SELECT c FROM Clientes c where dni = :dni", Clientes.class);
            queryClientes.setParameter("dni", dni);
            Clientes cliente = queryClientes.getSingleResult();
            System.out.println(cliente.toString());
            String nomec = cliente.getNomec();
            boolean descuento = cliente.getNcompras() > 0;

            TypedQuery<Vehiculos> queryVehiculos = em.createQuery("SELECT v FROM Vehiculos v where codveh = :codveh", Vehiculos.class);
            queryVehiculos.setParameter("codveh", codveh);
            Vehiculos vehiculo = queryVehiculos.getSingleResult();
            String nombreVehiculo = vehiculo.getNomveh();
            long prezoOrixe = vehiculo.getPrezoorixe();
            int anoMatricula = vehiculo.getAnomatricula();

            long prezoFinal = prezoOrixe - ((2019 - anoMatricula) * 500) - (descuento ? 500 : 0);


        }
        OracleDB.getSession().setAutoclose(true);
        mongoClient.close();
        em.close();
        emf.close();
    }
}