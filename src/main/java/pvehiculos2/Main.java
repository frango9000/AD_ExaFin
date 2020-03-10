package pvehiculos2;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import db.OracleDB;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import org.bson.Document;

public class Main {

    public static void main(String[] args) {
        OracleDB.getSession().setAutoclose(false);

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("src/main/resources/pvehiculos2/finalveh.odb");
        EntityManager em = emf.createEntityManager();

        //Eliminamos contenido de la tabla
        try {
            em.getTransaction().begin();
            Query clean = em.createQuery("DELETE FROM Venfin");
            clean.executeUpdate();
            em.getTransaction().commit();
            em.flush();
        } catch (PersistenceException e) {
            em.getTransaction().rollback();
            em.flush();
        }

        //verificamos que la tabla esta vacia
        printFinalvehTable(em);

        MongoClient mongoClient = new MongoClient(OracleDB.SERVER_HOSTNAME, 27017);
        MongoDatabase mongoDB = mongoClient.getDatabase("basevehiculos");
        MongoCollection<Document> clientes = mongoDB.getCollection("clientes");
        MongoCollection<Document> vehiculos = mongoDB.getCollection("vehiculos");

        try (Statement statement = OracleDB.getSession().getConn().createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM VENDAS");
            while (resultSet.next()) {
                //extraccion de datos en oracle vendas
                int id = resultSet.getInt(1);
                String dni = resultSet.getString(2);
                java.sql.Struct vehf = (java.sql.Struct) resultSet.getObject("vehf");
                Object[] attributes = vehf.getAttributes();
                String codveh = (String) attributes[0];
                BigDecimal tasas = (BigDecimal) attributes[1];

                //extraccion de datos en mongodb vehiculos
                Document vehiculo = vehiculos.find(Filters.eq("_id", codveh)).first();
                assert vehiculo != null;
                String nomveVeh = vehiculo.getString("nomveh");
                double prezoOrixe = vehiculo.getDouble("prezoorixe");
                double anoMatricula = vehiculo.getDouble("anomatricula");

                //extraccion de datos en mongodb clientes
                Document cliente = clientes.find(Filters.eq("_id", dni)).first();
                assert cliente != null;
                Object key;
                String nomec = cliente.getString("nomec");
                double nCompras = cliente.getDouble("ncompras");

                //calculo precio final
                boolean descuento = nCompras > 0;
                double prezoFinal = prezoOrixe - ((2019 - anoMatricula) * 500) - (descuento ? 500 : 0) + tasas.doubleValue();

                //insercion final en ObjectDB
                Venfin venfin = new Venfin((double) id, dni, nomec, nomveVeh, prezoFinal);
                em.getTransaction().begin();
                em.persist(venfin);
                em.flush();
                em.getTransaction().commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //imprimimos de nuevo el contenido para verificar el resultado
        printFinalvehTable(em);

        //cerramos mongo, objectdb y oracledb
        mongoClient.close();
        em.close();
        emf.close();
        OracleDB.getSession().setAutoclose(true);
    }

    //imprime la cuenta de filas  en objectdb tabla Venfin, y el contenido de dichas filas
    private static void printFinalvehTable(EntityManager em) {
        TypedQuery<Venfin> query = em.createQuery("SELECT v FROM Venfin v", Venfin.class);
        List<Venfin> results = query.getResultList();
        System.out.println("Contenido de tabla Venfin en finalveh.odb: " + results.size());
        for (Venfin v : results) {
            System.out.println(v);
        }
    }
}