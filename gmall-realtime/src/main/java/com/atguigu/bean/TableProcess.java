package com.atguigu.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


// 2022-06-08 15:15:58
//  lombok  减少java模板代码
//  @Data   为所有字段生成getter、有用的toString方法以及检查所有非瞬态字段的hashCode和equals实现。还将为所有非最终字段生成setter以及构造函数。等效于@Getter@Setter@RequiredArgsConstructor@ToString@EqualsAndHashCode。

//  @NonNull 用于标记类中不能允许为 null 的字段或者参数上，任何使用该字段的地方都生成空指针判断代码，若@NonNull 标记的变量为 null，抛出 NullPointException （NPE） 异常

//  @NoArgsConstructor 为实体类生成无参的构造器方法

//  @AllArgsConstructor 为实体类生成除了static修饰的字段之外带有各参数的构造器方法。

//  @RequiredArgsConstructor 为实体类生成指定字段的构造器方法，而这些字段需要被 final，或者 @NonNull修饰
//  @ToString 会给类自动生成易阅读的 toString 方法，带上有所非静态字段的属性名称和值，这样就十分便于我们日常开发时进行的打印操作。
//  @EqualsAndHashCode 注解就是用于根据类所拥有的非静态字段自动重写 equals 方法和 hashCode 方法,方便我们用于对象间的比较。用法和@ToString类似，同样也支持@EqualsAndHashCode.Exclude和@EqualsAndHashCode.Include，
//  @Builder 是一个非常强大的注解，提供了一种基于建造者模式的构建对象的 API。使用 @Builder 注解为给我们的实体类自动生成 builder() 方法，并且直接根据字段名称方法进行字段赋值，最后使用 build()方法构建出一个实体对象。
//          但是 @Builder 不支持父类字段的生成，当一个实体类存在父类时，@Builder 只能生成当前类的字段构建方法。
//  @SuperBuilder 需要用到父类的字段方法时

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    private String sourceTable;
    private String sinkTable;
    private String sinkColumns;
    private String sinkPk;
    private String sinkExtend;
}
