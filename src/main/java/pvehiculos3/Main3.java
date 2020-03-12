package pvehiculos3;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import db.OracleDB;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.bson.Document;

public class Main3 {

    public static void main(String[] args) {
        OracleDB.getSession().setAutoclose(false);

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("src/main/resources/pvehiculos3/vehicli.odb");
        EntityManager em = emf.createEntityManager();

        MongoClient mongoClient = new MongoClient(OracleDB.SERVER_HOSTNAME, 27017);
        MongoDatabase mongoDB = mongoClient.getDatabase("test");
        MongoCollection<Document> finalveh = mongoDB.getCollection("finalveh");

        finalveh.drop();
        System.out.println(finalveh.countDocuments());

        TypedQuery<Vendas> vendasTypedQuery = em.createQuery("SELECT v FROM Vendas v", Vendas.class);
        List<Vendas> ventas = vendasTypedQuery.getResultList();
        for (Vendas venta : ventas) {
            System.out.println(venta);
            String nomv = null;
            int prezoOrixe = -1;
            int anoMatricula = -1;
            String nomcli = null;
            boolean comprasPrevias = false;

            try (PreparedStatement preparedStatement = OracleDB.getSession().getConn().prepareStatement("SELECT * FROM vehiculos WHERE idv = ?")) {
                preparedStatement.setString(1, venta.codvh);
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    nomv = resultSet.getString("idv");
                    java.sql.Struct vehf = (java.sql.Struct) resultSet.getObject("datos");
                    Object[] attributes = vehf.getAttributes();
                    prezoOrixe   = ((BigDecimal) attributes[0]).intValue();
                    anoMatricula = ((BigDecimal) attributes[1]).intValue();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try (PreparedStatement preparedStatement = OracleDB.getSession().getConn().prepareStatement("SELECT * FROM clientes WHERE idcli = ?")) {
                preparedStatement.setString(1, venta.dni);
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    java.sql.Struct clienteObj = (java.sql.Struct) resultSet.getObject(2);
                    Object[] datosCliente = clienteObj.getAttributes();
                    nomcli         = (String) datosCliente[0];
                    comprasPrevias = ((BigDecimal) datosCliente[1]).intValue() > 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            if (nomcli != null && nomv != null && prezoOrixe > -1 && anoMatricula > -1)
                finalveh.insertOne(new Document("_id", venta.id)
                                       .append("dni", venta.dni)
                                       .append("nomcli", nomcli)
                                       .append("nomv", nomv)
                                       .append("prezofinal", (prezoOrixe - ((2019 - anoMatricula) * 500) - (comprasPrevias ? 500 : 0) + venta.tasas)));


        }

        System.out.println(finalveh.countDocuments());
        FindIterable<Document> testinit = finalveh.find();
        testinit.iterator().forEachRemaining(System.out::println);

        //cerramos mongo, objectdb y oracledb
        mongoClient.close();
        em.close();
        emf.close();
        OracleDB.getSession().setAutoclose(true);
    }

    static void queryFields(Document query, String... fields) {
        for (String field : fields) {
            System.out.println(field + ": " + query.get(field));
        }
    }
}
