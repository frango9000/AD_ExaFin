package pvehiculos;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.bson.Document;

public class Main {
    public static EntityManager em;
    public static void main(String[] args) {
        

        
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("src/main/resources/pvehiculos/vehicli.odb");
        EntityManager em = emf.createEntityManager();
        
        MongoClient mongoClient = new MongoClient("10.0.9.119", 27017);
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("vendas");

        for (Document document : collection.find()) {
            System.out.println(document);
            int id = document.getInteger("_id");
            String dni  = document.getString("dni");
            String codveh = document.getString("codeveh");

            TypedQuery<Clientes> queryClientes = em.createQuery("SELECT c FROM Clientes c where dni = :dni", Clientes.class);
            queryClientes.setParameter("dni", dni);
            Clientes cliente = queryClientes.getSingleResult();
            String nombre = cliente.getNomec();
            boolean descuento = cliente.getNcompras() > 0;

            TypedQuery<Vehiculos> queryVehiculos = em.createQuery("SELECT v FROM Vehiculos v where codveh = :codveh", Vehiculos.class);
            queryVehiculos.setParameter("codveh", codveh);
            Vehiculos vehiculo = queryVehiculos.getSingleResult();
            String nombreVehiculo = vehiculo.getNomveh();
            long prezoOrixe = vehiculo.getPrezoorixe();
            int anoMatricula = vehiculo.getAnomatricula();
            
            long prezoFinal = prezoOrixe - ( ( 2019 - anoMatricula) * 500) - ( descuento ? 500 : 0);



            PreparedStatement stmt = connection.prepareStatement("insert into finalveh values (?,?,?,tipo_vehf(?,?))");

            stmt.setInt(1,id);
            stmt.setString(2,dni);
            stmt.setString(3,nombre);
            stmt.setString(4,marca);
            stmt.setInt(5,numero);

            stmt.executeUpdate();






        }
        // Close the database connection:
        em.close();
        emf.close();

    }

    private static void queryPrintVehiculos() {
        TypedQuery<Vehiculos> query = em.createQuery("SELECT v FROM Vehiculos v", Vehiculos.class);
        List<Vehiculos> results = query.getResultList();
        System.out.println(results.size());
        for (Vehiculos vehiculo : results) {
            System.out.println(vehiculo);
        }
        System.out.println();
    }
    private static void queryPrintClientes() {
        TypedQuery<Clientes> query = em.createQuery("SELECT v FROM Clientes v", Clientes.class);
        List<Clientes> results = query.getResultList();
        System.out.println(results.size());
        for (Clientes cliente : results) {
            System.out.println(cliente);
        }
        System.out.println();
    }

    public static Connection getConexion() throws SQLException{

        try (Connection connection = DriverManager.getConnection("jdbc:<db_url>", "<username>", "<password>");
             PreparedStatement preparedStatement = connection.prepareStatement("<changeme>")) {
            // ... add parameters to the SQL query using PreparedStatement methods:
            //     setInt, setString, etc.
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    // ... do something with result set

                }
            }
        } catch (SQLException e) {
            // ... handle SQL exception
        }


        String usuario = "hr";
        String password = "hr";
        String host = "localhost";
        String puerto = "1521";
        String sid = "orcl";
        String ulrjdbc = "jdbc:oracle:thin:" + usuario + "/" + password + "@" + host + ":" + puerto + ":" + sid;

//        conexion = DriverManager.getConnection(ulrjdbc);
//        return conexion;
    }

}
