import java.io.Serializable;

public class Person implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String name;
	public int num;
	public double balance;
	public Boolean is_vip;
	@Override
	public String toString() {
		return "Person [name=" + name + ", num=" + num + ", balance=" + balance + ", is_vip=" + is_vip + "]";
	}

}
