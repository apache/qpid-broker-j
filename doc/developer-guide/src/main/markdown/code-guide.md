# Qpid Broker-J Coding Standards

This article documents the standard adopted for Java code in the Qpid project.
All committers are expected to follow this standard.

<!-- toc -->

- [Executive Summary](#executive-summary)
- [Details](#details)
  * [Introduction](#introduction)
  * [Source files](#source-files)
  * [Java Elements](#java-elements)
    + [Class definitions](#class-definitions)
    + [Variables](#variables)
    + [Methods](#methods)
  * [Expressions](#expressions)
  * [Statements](#statements)
    + [Simple Statements](#simple-statements)
    + [Compound Statements](#compound-statements)
  * [General](#general)
  * [Exceptions](#exceptions)

<!-- tocstop -->

## Executive Summary

The main things for layout purposes in the standard are:

 * Indent using four spaces. No tabs.
 * braces always go on new lines, e.g.
```java
if (x == 5)
{
    System.out.println("Hello");
}
```

rather than

```java
if (x == 5} {
    System.out.println("Hello");
}
```

Always add braces, e.g.

```java
    if (x == 5)
    {
        System.out.println("Hello");
    }
```
rather than

```java
if (x == 5}
    System.out.println("Hello");
```

Fields prefixed with underscores, e.g. `_messageCount`

Spaces after keywords but no spaces either before or after parentheses in method calls, e.g.

```java
    if (x == 5)
```

rather than

```java
    if(x==5)
```

but

```java
    foo.bar(4, 5)
```

rather than

```java
    foo.bar( 4, 5 )
```

## Details

### Introduction

This document describes two types of coding standard:

1. **Mandatory** standards must be followed at all times.
2. **Recommended** standards should in general be followed but in particular cases may be omitted
   where the programmer feels that there is a good reason to do so.

Code that does not adhere to mandatory standards will not pass the automated checks
(or a code review if the guideline is not stylistic).

### Source files

This section defines the general rules associated with the contents of a Java source file and the order
in which the each part should be presented. No rules on programming style, naming conventions or indentation are given here.

1. Java source files must have a ".java" suffix (this will be enforced by the compiler) [mandatory].
2. The basename of a Java source file must be the same as the public class defined therein
   (this will be enforced by the compiler) [mandatory].
3. Only one class should be defined per source file (except for inner classes and one-shot uses
   where the non-public class cannot conceivably be used outside of its context) [mandatory].
4. Source files should not exceed 1500 lines [recommended].
5. No line in a source file should exceed 120 characters [mandatory].
6. The sections of a source file should be presented in the following order [mandatory]:
   * File information comment (see rule 7 below).
   * Package name (see rule 8 below).
   * Imports (see rules 9 to 10 below).
   * Other class definitions.
   * Public class definition.
7. Do not use automatically expanded log or revision number provided by your source code management system
   unless it provides a facility to avoid "false conflicts" when doing merges due simply to revision number changes
   (which happens, for example, with cvs when branches are used). [mandatory]
8. Every class that is to be released must be a member of a package [mandatory].
    Rationale: classes that are not explicitly put in a package are placed in the unnamed package by the compiler.
    Therefore as the classes from many developers will be being placed in the same package the likelihood of a name
    clash is greatly increased.
9. All class imports from the same package should be grouped together. A single blank line should separate imports
   from different packages [recommended].
10. Wildcard imports should be avoided as they could lead to conflicts between classes in different packages
    with the same name. [recommended].
11. Use javadoc tags and use HTML mark-up to enhance the readability of the output files [mandatory].

### Java Elements

This section gives advice on coding the various elements of the Java programming language.

#### Class definitions

This section gives guidelines for class and interface definitions in Java.
The term class in this section is used more broadly to mean class and interface:

1. Class names should start with a capital letter with every subsequent word capitalised,
   for example: `DataProcessor` [mandatory].
2. All classes should be preceded by a javadoc comment describing the purpose of the class [recommended].
3. Class-level javadoc comments should specify the thread-safety of the class [recommended].
4. The name of exception classes should end in the word exception, for example: UnknownMungeException [mandatory].
5. Class names should in general not be overloaded. For example, defining a class "com.foo.bar.String"
    should be avoided as there is already a class "java.lang.String" [recommended].
    Rationale: adhering to this rule reduces the likelihood of confusion and means that the use of fully qualified
    class names should not be required.
6. The definition of the primary class (i.e. the class with the same name as the java file) should start in column 0
   of the source file. Inner class definitions should be indented 4 spaces more than their enclosing class [mandatory].
7. Declare a class as final only if specialisation will never be required and improved performance is essential.
   With modern JVMs there in fact may be no performance advantage. Warning: use of final limits code reuse [mandatory].
8. For all but simplest classes the following methods should have useful definitions [recommended]:
    ```java
        public boolean equals(Object obj)
        public int hashCode()
        public String toString()
    ```
9. The order of presentation of the sections in a class should be [mandatory]:
   * Variables
   * Methods

#### Variables
This section gives guidelines for class and instance variable definitions in Java.
In this section if a rule uses the term variable rather than instance variable or class variable,
then the rule applies to both types of variable.

1. The order of presentation of variables in a class definition should be [recommended]:
    * private, protected, public: static final variables (aka constant class variables).
    * private, protected, public: static variables (aka class variables).
    * private, protected, public: final variables (aka constant instance variables).
    * private, protected, public: variables (aka instance variables).
    It should be noted that as javadoc will automatically order variables in a consistent manner,
    rigid adherence to this rule is not necessary.
2. Variable modifiers should be presented in the following order: static, final, transient, volatile [mandatory].
3. The names of static final variables should be upper case with subsequent words prefixed with an underscore [mandatory].
   For example:
    ```java
    public static final int NOT_FOUND = -1;
    ```
4. When a subclass refers to a static final variable defined in a parent class, access should be qualified
    by specifying the defining class name [mandatory].
    For example: use `ParentClass.MAX` rather than `MAX`.
5. The names of variables (other that static final) should start with a lower case letter.
   Any words that are contained in the rest of the variable name should be capitalised [mandatory].
    For example:
    ```java
    String name;
    String[] childrensNames;
     ```
6. Class and instance variables must be prefixed with an underscore (_) [mandatory].
7. Variables must not be named using the so-called Hungarian notation [mandatory].
   For example:
    ```java
    int nCount = 4; // not allowed
    ```
8. Only one variable may be defined per line [mandatory].
9. Variable declarations should be indented 4 spaces more than their enclosing class [mandatory].
10. Class instance variables might be preceded by a javadoc comment that specifies what the variable is for,
    where it is used and so forth. Though, the self-explanatory variable name should be preferred over comments.
    If comment is necessary, it should be indented to the same level as the variable it refers to [recommended]
11. Never declare instance variables as public unless the class is effectively a "struct" [mandatory].
12. Never give a variable the same name as a variable in a superclass [mandatory].
13. Ensure that all non-private class variables have sensible values even if no instances have been created
    (use static initialisers if necessary, i.e.`static { ... }`) [mandatory].
    Rationale: prevents other objects accessing fields with undefined/unexpected values.

#### Methods
This section gives guidelines for class and instance method definitions in Java.
In this section if a rule uses the term method rather than instance method or class method,
then the rule applies to both types of method.

1. Constructors and `finalize` methods should follow immediately after the variable declarations [mandatory].
2. Do not call non-final methods from constructors. This can lead to unexpected results when the class is subclassed.
   If you must call non-final methods from constructors, document this in the constructor's javadoc [mandatory].
   Note that private implies final.
3. Methods that are associated with the same area of functionality should be physically close to one another [recommended].
4. After grouping by functionality, methods should be presented in the following order [recommended]:
   * private, protected, public: static methods.
   * private, protected, public: instance methods.
    It should be noted that as javadoc will automatically order methods in a consistent manner,
    rigid adherence to this rule is not necessary.
5. Method modifiers should be presented in the following order: abstract, static, final, synchronized [mandatory]
6. When a synchronized method is overloaded, it should be explicitly synchronized in the subclass [recommended].
7. Method names should start with a lower case letter with all subsequent words being capitalised [mandatory].
   For example:
    ```java
    protected int resize(int newSize)
    protected void addContentsTo(Container destinationContainer)
    ```
8. Methods which get and set values should be named as follows [mandatory]:
    ```java
    Type getVariableName()
    void setVariableName(Type newValue)
    ```
    Exceptions should be used to report any failure to get or set a value.
    The `@param` description should detail any assumptions made by the implementation,
     for example: "Specifying a null value will cause an error to be reported".
9. Method definitions should be indented 4 spaces more than their enclosing class [mandatory].
10. All non-private methods should be preceded by a javadoc comment specifying what the method is for,
    detailing all arguments, returns and possible exceptions [mandatory]
11. The braces associated with a method should be on a line on their own and be indented to the same level
    as the method [mandatory]. For example:
    ```java
    public void munge()
    {
        int i;
        // method definition omitted...
    }
    ```
12. The body of a method should be indented 4 columns further that the opening and closing braces associated with it [mandatory].
    See the above rule for an example.
13. When declaring and calling methods there should be no white space before or after the parenthesis [mandatory].
14. In argument lists there should be no white space before a comma, and only a single space (or newline) after it [mandatory].
    For example:
    ```java
    public void munge(int depth, String name)
    {
        if (depth > 0)
        {
            munge(depth - 1, name);
        }
        // do something
    }
    ```
15. Wherever reasonable define a default constructor (i.e. one that takes no arguments)
    so that `Class.newInstance()` may be used [recommended]. If an instance which was created by default construction
    could be used until further initialisation has been performed, then all unserviceable requests should cause
    a runtime exception to be thrown.
16. The method public static void main() should not be used for test purposes.
    Instead a test/demo program should be supplied separately. [mandatory].
17. Public access methods (i.e. methods that get and set attributes) should only be supplied when required [mandatory].
18. If an instance method has no natural return value, declare it as void rather than using the "return this;"
    convention [mandatory].
19. Ensure that non-private static methods behave sensibly if no instances of the defining class have been created [mandatory].

### Expressions
This section defines the rules to be used for Java expressions:

1. Unary operators should not be separated from their operand by white space [mandatory].
2. Embedded `++` or `--` operators should only be used when it improves code clarity [recommended]. This is rare.
3. Extra parenthesis should be used in expressions to improve their clarity [recommended].
4. The logical expression operand of the `?:` (ternary) operator must be enclosed in parenthesis.
   If the other operands are also expressions then they should also be enclosed in parenthesis [mandatory]. For example:
    ```java
    biggest = (a > b) ? a : b;
    complex = (a + b > 100) ? (100 * c) : (10 * d);
    ```
5. Nested `?:` (ternary) operators can be confusing and should be avoided [mandatory].
6. Use of the binary "," operator (the comma operator) should be avoided [mandatory].
   Putting all the work of a for loop on a single line is not a sign of great wisdom and talent.
7. If an expression is too long for a line (i.e. extends beyond column 119) then it should be split after the lowest
   precedence operator near the break [mandatory]. For example:
    ```java
    if ((state == NEED_TO_REPLY) ||
        (state == REPLY_ACK_TIMEOUT))
    {
        // (re)send the reply and enter state WAITING_FOR_REPLY_ACK
    }
    ```
    Furthermore if an expression requires to be split more than once, then the split should occur at the same logical
    level if possible.
8. All binary and ternary operators (exception for ".") should be separated from their operands by a space [mandatory].

### Statements

#### Simple Statements

This section defines the general rules for simple Java statements:

1. There must only be one statement per line [mandatory].
2. In general local variables should be named in a similar manner to instance variables [recommended].
3. More than one temporary variable may be declared on a single line provided no initialisers are used [mandatory].
   For example:
    ```java
    int j, k = 10, l;  // Incorrect!
    int j, l;          // Correct
    int k = 10;
     ```
4. A null body for a while, for, if, etc. should be documented so that it is clearly intentional [mandatory].
5. Keywords that are followed by a parenthesised expression (such as while, if, etc) should be separated from
   the open bracket by a single space [mandatory]. For example:
    ```java
    if (a > b)
    {
        munge();
    }
     ```
6. In method calls, there should be no spaces before or after the parentheses [mandatory]. For example:
    ```java
    munge (a, 10);    // Incorrect!
    munge(a, 10);     // Correct.
     ```
#### Compound Statements
This section defines the general rules associated with compound statements in Java:

1.  The body of a compound statement should be indented by 4 spaces more than the enclosing braces [mandatory].
    See the following rule for an example.
2.  The braces associated with a compound statement should be on their own line and be indented to the same level
    as the surrounding code [mandatory]. For example:
    ```java
    if ((length >= LEN_BOX) && (width >= WID_BOX))
    {
        int i;
        // Statements omitted...
    }
     ```
3.  If the opening and closing braces associated with a compound statement are further than 20 lines apart
     then the closing brace should annotated as follows [mandatory]:
    ```java
    for (int j = 0; j < SIZE; j++)
    {
        lotsOfCode();
    } // end for
     ```
4. All statements associated with an if or if-else statement should be made compound by the use of braces [mandatory].
   For example:
    ```java
    if (a > b)
    {
        statement();
    }
    else
    {
        statement1();
        statement2();
    }
     ```
5. The case labels in a switch statement should be on their own line and indented by a further 4 spaces.
   The statements associated with the label should be indented by 4 columns more than the label and not be enclosed
    in a compound statement. [mandatory]. For example:
    ```java
    switch (tState)
    {
        case NOT_RUNNING:
            start();
            break;
        case RUNNING:
        default:
            monitor();
            break;
    }
     ```
6.  In switch statements - the statements associated with all cases should terminate with a statement
    which explicitly determines the flow of control, for example `break` [recommended].
7.  In switch statements - fall through should be avoided wherever possible, however if it is unavoidable it must
    be commented with `// FALLTHROUGH` [mandatory].
8.  In switch statements - a default case must be present and should always be the last case [mandatory].

### General
This section gives general rules to be followed when programming in Java:

1.  When comparing objects for equivalence use the method equals() and not the == operator.
    The only exceptions to this are static final objects that are being used as constants and interned Strings [mandatory].
2.  In general labelled break and continue statements should be avoided [recommended].
    This is due to the complex flow of control, especially when used with try/finally blocks.
3.  Unless some aspect of an algorithm relies on it, then loops count forward [mandatory]. For example:
    ```java
    for (int j = 0; j < size; j++)
    {
        // Do something interesting
    }
     ```
4.  Use local variables in loops [recommended]. For example:
    ```java
    ArrayList clone = (ArrayList)listeners.clone();
    final int size = clone.size();
    for (int j = 0; j < size; j++)
    {
        System.out.println(clone.elementAt(j));
    }
     ```
5.  Anonymous inner classes should define no instance variables and be limited to three single line methods.
    Inner classes that declare instance variables or have more complex methods should be named [mandatory].
6.  Use final local variables where possible to help avoid errors in code [recommended]. For example:
    ```java
    public void foo()
    {
        final int x = dataSource.getCount();
        // do things with x
        // ...
    }
     ```
7.  To indicate that further work is intended on a section of code, add a comment prefixed by "TODO" explaining
    what needs to be done and why [recommended].
8.  If code is so incomplete that executing it would lead to incorrect or confusing results,
    throw UnsupportedOperationException with an explanatory message [mandatory].

### Exceptions

This section gives general guidance on the use of exceptions when programming in Java.

1.  try/catch blocks should be laid out like any other compound statement [mandatory]. For example:
    ```java
    try
    {
        String str = someStrings[specifiedIndex];
    }
    catch (IndexOutOfBoundsException ex)
    {
        // The user specified an incorrect index, better take
        // some remedial action.
    }
     ```
2. When an exception is caught but ignored then a comment should be supplied explaining the rationale
   (note that this rule includes InterruptedException, which should almost never be ignored) [mandatory]. For example:
    ```java
    try
    {
        propertySet.setProperty("thingy", new Integer(10));
    }
    catch (UnknownPropertyException ignore)
    {
        // This exception will never occur as "thingy" definitely exists
    }
     ```
3.  All exceptions that are likely to be thrown by a method should be documented,
    except if they are runtime exceptions (note: the compiler will not enforce catch blocks for runtimes even
    if they are mentioned in the throws clause) [mandatory]. For example:
    ```java
    /* Comment snippet:
     * @exception IllegalValueException Thrown if values is null or
     *     any of the integers it contains is null.
     */
    private Integer sum(Integer[] values) throws IllegalValueException
     ```
