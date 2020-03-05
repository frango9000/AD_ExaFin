
package pvehiculos;
import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.Id;
@Entity
public class Clientes implements Serializable{
   private static final long serialVersionUID = 1L; 
    @Id 
    String dni  ;		
    String nomec ;	
    int ncompras; 

    public Clientes() {
    }

    public Clientes(String dni, String nomec, int ncompras) {
        this.dni = dni;
        this.nomec = nomec;
        this.ncompras = ncompras;
    }

    public String getDni() {
        return dni;
    }

    public void setDni(String dni) {
        this.dni = dni;
    }

    public String getNomec() {
        return nomec;
    }

    public void setNomec(String nomec) {
        this.nomec = nomec;
    }

    public int getNcompras() {
        return ncompras;
    }

    public void setNcompras(int ncompras) {
        this.ncompras = ncompras;
    }

    @Override
    public String toString() {
        return "Clientes{" + "dni=" + dni + ", nomec=" + nomec + ", ncompras=" + ncompras + '}';
    }

//    private static void queryPrintVehiculos() {
//        TypedQuery<Vehiculos> query = em.createQuery("SELECT v FROM Vehiculos v", Vehiculos.class);
//        List<Vehiculos> results = query.getResultList();
//        System.out.println(results.size());
//        for (Vehiculos vehiculo : results) {
//            System.out.println(vehiculo);
//        }
//        System.out.println();
//    }
//    private static void queryPrintClientes() {
//        TypedQuery<Clientes> query = em.createQuery("SELECT v FROM Clientes v", Clientes.class);
//        List<Clientes> results = query.getResultList();
//        System.out.println(results.size());
//        for (Clientes cliente : results) {
//            System.out.println(cliente);
//        }
//        System.out.println();
//    }
}
