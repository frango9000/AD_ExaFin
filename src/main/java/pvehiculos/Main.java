package pvehiculos;

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

public class Main {
    public static EntityManager em;
    public static void main(String[] args) {
        //conectarse a base
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("src/main/resources/pvehiculos/vehicli.odb");
        em = emf.createEntityManager();

//        Query q1 = em.createQuery("SELECT v FROM Vehiculos v");
//        System.out.println("Total Points: " + q1.getSingleResult());

        // listado de todos os obxectos
        queryPrintVehiculos();
        queryPrintClientes();

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

}
