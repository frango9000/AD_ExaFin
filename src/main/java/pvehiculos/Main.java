package pvehiculos;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.bson.Document;

public class Main {
    public static void main(String[] args) {
        OracleDB.getSession().setAutoclose(false);

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("src/main/resources/pvehiculos/vehicli.odb");
        EntityManager em = emf.createEntityManager();

        MongoClient mongoClient = new MongoClient(OracleDB.SERVER_HOSTNAME, 27017);
        MongoDatabase mongoDB = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = mongoDB.getCollection("vendas");
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

            try (PreparedStatement preparedStatement = OracleDB.getSession().getConn().prepareStatement("INSERT INTO FINALVEH VALUES (?, ?, ?, TIPO_VEHF(?, ?))")) {
                preparedStatement.setInt(1, id);
                preparedStatement.setString(2, dni);
                preparedStatement.setString(3, nomec);
                preparedStatement.setString(4, nombreVehiculo);
                preparedStatement.setLong(5, prezoFinal);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        OracleDB.getSession().setAutoclose(true);
        mongoClient.close();
        em.close();
        emf.close();
    }
}